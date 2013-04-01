package org.elasticsearch.river.hbase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.facet.FacetBuilders;
import org.elasticsearch.search.facet.statistical.StatisticalFacet;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;

/**
 * A separate Thread that does the actual fetching and storing of data from an HBase cluster.
 * 
 * @author Ravi Gairola
 */
class HBaseParser implements Runnable {
	private static final String	TIMESTMAP_STATS	= "timestamp_stats";
	private final HBaseRiver	river;
	private final ESLogger		logger;
	private int					indexCounter;
	private HBaseClient			client;
	private Scanner				scanner;
	private boolean				stopThread;

	HBaseParser(final HBaseRiver river) {
		this.river = river;
		this.logger = river.getLogger();
	}

	/**
	 * Timing mechanism of the thread that determines when a parse operation is supposed to run. Waits for the predefined
	 * interval until a new run is performed. The method checks every 1000ms if it should be parsing again. The first run is
	 * done immediately once the thread is started.
	 */
	@Override
	public void run() {
		this.logger.info("HBase Import Thread has started");
		long lastRun = 0;
		while (!this.stopThread) {
			if (lastRun + this.river.getInterval() < System.currentTimeMillis()) {
				lastRun = System.currentTimeMillis();
				try {
					this.indexCounter = 0;
					parse();
				} catch (Throwable t) {
					this.logger.error("An exception has been caught while parsing data from HBase", t);
				}
				if (!this.stopThread) {
					this.logger.info("HBase Import Thread is waiting for {} Seconds until the next run", this.river.getInterval() / 1000);
				}
			}
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				this.logger.trace("HBase river parsing thread has been interrupted");
			}
		}
		this.logger.info("HBase Import Thread has finished");
	}

	/**
	 * The actual parse implementation that connects to the HBase cluster and fetches all rows since the last import. Fetched
	 * rows are added to an ElasticSearch Bulk Request with a size according to batchSize (default is 100).
	 * 
	 * @throws InterruptedException
	 * @throws Exception
	 */
	protected void parse() throws InterruptedException, Exception {
		this.logger.info("Parsing data from HBase");
		try {
			this.client = new HBaseClient(this.river.getHosts());
			this.logger.debug("Checking if table {} actually exists in HBase DB", this.river.getTable());
			this.client.ensureTableExists(this.river.getTable());
			this.logger.debug("Fetching HBase Scanner");
			this.scanner = this.client.newScanner(this.river.getTable());
			this.scanner.setServerBlockCache(false);
			if (this.river.getFamily() != null) {
				this.scanner.setFamily(this.river.getFamily());
			}
			if (this.river.getQualifiers() != null) {
				for (final String qualifier : this.river.getQualifiers().split(",")) {
					this.scanner.setQualifier(qualifier.trim().getBytes(this.river.getCharset()));
				}
			}

			setMinTimestamp(this.scanner);
			ArrayList<ArrayList<KeyValue>> rows;
			this.logger.debug("Starting to fetch rows");

			while ((rows = this.scanner.nextRows(this.river.getBatchSize()).joinUninterruptibly()) != null) {
				if (this.stopThread) {
					this.logger.info("Stopping HBase import in the midle of it");
					break;
				}
				parseBulkOfRows(rows);
			}
		} finally {
			this.logger.debug("Closing HBase Scanner and Async Client");
			if (this.scanner != null) {
				try {
					this.scanner.close();
				} catch (Exception e) {
					this.logger.error("An Exception has been caught while closing the HBase Scanner", e, (Object[]) null);
				}
			}
			if (this.client != null) {
				try {
					this.client.shutdown();
				} catch (Exception e) {
					this.logger.error("An Exception has been caught while shuting down the HBase client", e, (Object[]) null);
				}
			}
		}
	}

	/**
	 * Run over a bulk of rows and process them.
	 * 
	 * @param rows
	 */
	protected void parseBulkOfRows(final ArrayList<ArrayList<KeyValue>> rows) {
		this.logger.debug("Processing the next {} entries in HBase parsing process", rows.size());
		final BulkRequestBuilder bulkRequest = this.river.getEsClient().prepareBulk();
		for (final ArrayList<KeyValue> row : rows) {
			if (this.stopThread) {
				this.logger.info("Stopping HBase import in the midle of it");
				break;
			}
			if (row.size() > 0) {
				final IndexRequestBuilder request = this.river.getEsClient().prepareIndex(this.river.getIndex(), this.river.getType());
				request.setSource(readDataTree(row));
				request.setTimestamp(String.valueOf(row.get(0).timestamp()));
				if (this.river.getIdField() == null) {
					request.setId(new String(row.get(0).key(), this.river.getCharset()));
				}
				bulkRequest.add(request);
			}
		}
		final BulkResponse response = bulkRequest.execute().actionGet();

		this.indexCounter += response.items().length;
		this.logger.info("HBase river has indexed {} entries so far", this.indexCounter);
		if (response.hasFailures()) {
			this.logger.error("Errors have occured while trying to index new data from HBase");
		}
	}

	/**
	 * Generate a tree structure that ElasticSearch can read and index from one of the rows that has been returned from
	 * HBase.
	 * 
	 * @param row
	 * @return
	 */
	@SuppressWarnings("unchecked")
	protected Map<String, Object> readDataTree(final ArrayList<KeyValue> row) {
		final Map<String, Object> dataTree = new HashMap<String, Object>();
		for (final KeyValue column : row) {
			final String family = this.river.normalizeField(new String(column.family(), this.river.getCharset()));
			final String qualifier = new String(column.qualifier(), this.river.getCharset());
			final String value = new String(column.value(), this.river.getCharset());
			if (!dataTree.containsKey(family)) {
				dataTree.put(family, new HashMap<String, Object>());
			}
			readQualifierStructure((Map<String, Object>) dataTree.get(family), qualifier, value);
		}
		return dataTree;
	}

	/**
	 * Will separate a column into sub column and return the value at the right json tree level.
	 * 
	 * @param parent
	 * @param qualifier
	 * @param value
	 */
	@SuppressWarnings("unchecked")
	protected void readQualifierStructure(final Map<String, Object> parent, final String qualifier, final String value) {
		if (this.river.getColumnSeparator() != null && !this.river.getColumnSeparator().isEmpty()) {
			final int separatorPos = qualifier.indexOf(this.river.getColumnSeparator());
			if (separatorPos != -1) {
				final String parentQualifier = this.river.normalizeField(qualifier.substring(0, separatorPos));
				final String childQualifier = qualifier.substring(separatorPos + this.river.getColumnSeparator().length());
				if (!childQualifier.isEmpty()) {
					if (!(parent.get(parentQualifier) instanceof Map)) {
						parent.put(parentQualifier, new HashMap<String, Object>());
					}
					readQualifierStructure((Map<String, Object>) parent.get(parentQualifier), childQualifier, value);
					return;
				}
				parent.put(this.river.normalizeField(qualifier.replace(this.river.getColumnSeparator(), "")), value);
				return;
			}
		}
		parent.put(this.river.normalizeField(qualifier), value);
	}

	/**
	 * Checks if there is an open Scanner or Client and closes them.
	 */
	public synchronized void stopThread() {
		this.stopThread = true;
	}

	/**
	 * Sets the minimum time stamp on the HBase scanner, by looking into Elasticsearch for the last entry made.
	 * 
	 * @param scanner
	 */
	protected long setMinTimestamp(final Scanner scanner) {
		this.logger.debug("Looking into ElasticSearch to determine timestamp of last import");
		final SearchResponse response = this.river.getEsClient()
			.prepareSearch(this.river.getIndex())
			.setTypes(this.river.getType())
			.setQuery(QueryBuilders.matchAllQuery())
			.addFacet(FacetBuilders.statisticalFacet(TIMESTMAP_STATS).field("_timestamp"))
			.execute()
			.actionGet();

		if (response.facets().facet(TIMESTMAP_STATS) != null) {
			this.logger.debug("Got statistical data from ElasticSearch about data timestamps");
			final StatisticalFacet facet = (StatisticalFacet) response.facets().facet(TIMESTMAP_STATS);
			final long timestamp = (long) Math.max(facet.getMax() + 1, 0);
			scanner.setMinTimestamp(timestamp);
			this.logger.debug("Found latest timestamp in ElasticSearch to be {}", timestamp);
			return timestamp;
		}
		this.logger.debug("No statistical data about data timestamps could be found -> probably no data there yet");
		scanner.setMinTimestamp(0);
		this.logger.debug("Found latest timestamp in ElasticSearch to be not present (-> 0)");
		return 0L;
	}
}