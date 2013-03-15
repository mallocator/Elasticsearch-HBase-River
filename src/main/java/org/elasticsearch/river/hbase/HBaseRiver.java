package org.elasticsearch.river.hbase;

import java.lang.Thread.UncaughtExceptionHandler;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Map;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.search.facet.FacetBuilders;
import org.elasticsearch.search.facet.statistical.StatisticalFacet;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;

public class HBaseRiver extends AbstractRiverComponent implements River, UncaughtExceptionHandler {
	private final Client	esClient;
	private volatile Thread	thread;

	private final String	hosts;
	private final String	table;
	private final String	index;
	private final String	type;
	private final long		interval;
	private final int		batchSize;
	private final String	idField;

	@Inject
	public HBaseRiver(final RiverName riverName, final RiverSettings settings, final Client esClient) {
		super(riverName, settings);
		this.esClient = esClient;
		this.logger.info("Creating HBase Stream River");

		this.hosts = readConfig("hosts", readConfig("zookeeper", null));
		this.table = readConfig("table");
		this.idField = readConfig("idField", null);
		this.index = readConfig("index", riverName.name());
		this.type = readConfig("type", "data");
		this.interval = Long.parseLong(readConfig("interval", "600000"));
		this.batchSize = Integer.parseInt(readConfig("batchSize", "1000"));
	}

	private String readConfig(final String config) {
		final String result = readConfig(config, null);
		if (result == null) {
			this.logger.error("Unable to read required config {}. Aborting!", config);
			throw new InvalidParameterException("Unable to read required config " + config);
		}
		return result;
	}

	@SuppressWarnings({ "unchecked" })
	private String readConfig(final String config, final String defaultValue) {
		if (this.settings.settings().containsKey("hbase")) {
			Map<String, Object> mysqlSettings = (Map<String, Object>) this.settings.settings().get("hbase");
			return XContentMapValues.nodeStringValue(mysqlSettings.get(config), defaultValue);
		}
		return defaultValue;
	}

	@Override
	public synchronized void start() {
		if (this.thread != null) {
			this.logger.warn("Trying to start HBase stream although it is already running");
			return;
		}
		this.logger.info("Starting HBase Stream");
		String mapping;
		if (this.idField == null) {
			mapping = "{\"" + this.type + "\":{\"_timestamp\":{\"enabled\":true}}}";
		}
		else {
			mapping = "{\"" + this.type + "\":{\"_timestamp\":{\"enabled\":true},\"_id\":{\"path\":\"" + this.idField + "\"}}}";
		}

		try {
			this.esClient.admin().indices().prepareCreate(this.index).addMapping(this.type, mapping).execute().actionGet();
			this.logger.info("Created Index {} with _timestamp mapping for {}", this.index, this.type);
		} catch (Exception e) {
			if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
				this.logger.debug("Not creating Index {} as it already exists", this.index);
			}
			else if (ExceptionsHelper.unwrapCause(e) instanceof ElasticSearchException) {
				this.logger.debug("Mapping {}.{} already exists and will not be created", this.index, this.type);
			}
			else {
				this.logger.warn("failed to create index [{}], disabling river...", e, this.index);
				return;
			}
		}

		try {
			this.esClient.admin()
				.indices()
				.preparePutMapping(this.index)
				.setType(this.type)
				.setSource(mapping)
				.setIgnoreConflicts(true)
				.execute()
				.actionGet();
		} catch (ElasticSearchException e) {
			this.logger.debug("Mapping already exists for index {} and type {}", this.index, this.type);
		}

		this.thread = EsExecutors.daemonThreadFactory(this.settings.globalSettings(), "hbase_slurper").newThread(new Parser());
		this.thread.setUncaughtExceptionHandler(this);
		this.thread.start();
	}

	@Override
	public void close() {
		this.logger.info("Closing HBase river");
		((Parser) this.thread).stopThread();
		this.thread = null;
	}

	@Override
	public void uncaughtException(final Thread arg0, final Throwable arg1) {
		this.logger.error("An Exception has been thrown in HBase Import Thread", arg1, (Object[]) null);
		close();
	}

	private class Parser extends Thread implements ActionListener<BulkResponse> {
		private static final String	TIMESTMAP_STATS	= "timestamp_stats";
		private int					indexCounter;
		private HBaseClient			client;
		private Scanner				scanner;
		private boolean				stopThread;

		private Parser() {}

		@Override
		public void run() {
			HBaseRiver.this.logger.info("HBase Import Thread has started");
			long lastRun = 0;
			while (!this.stopThread) {
				if (lastRun + HBaseRiver.this.interval < System.currentTimeMillis()) {
					lastRun = System.currentTimeMillis();
					try {
						parse();
					} catch (Exception e) {
						HBaseRiver.this.logger.error("An exception has been caught while parsing data from HBase", e);
					}
					if (HBaseRiver.this.interval <= 0) {
						break;
					}
					if (!this.stopThread) {
						HBaseRiver.this.logger.info("HBase Import Thread is waiting for {} Seconds until the next run",
							HBaseRiver.this.interval / 1000);
					}
				}
				try {
					sleep(1000);
				} catch (InterruptedException e) {}
			}
			HBaseRiver.this.logger.info("HBase Import Thread has finished");
		}

		private void parse() throws InterruptedException, Exception {
			HBaseRiver.this.logger.info("Parsing data from HBase");
			this.client = new HBaseClient(HBaseRiver.this.hosts);
			try {
				HBaseRiver.this.logger.debug("Checking if table {} actually exists in HBase DB", HBaseRiver.this.table);
				this.client.ensureTableExists(HBaseRiver.this.table);
				HBaseRiver.this.logger.debug("Fetching HBase Scanner");
				this.scanner = this.client.newScanner(HBaseRiver.this.table);
				setMinTimestamp(this.scanner);
				ArrayList<ArrayList<KeyValue>> rows;
				HBaseRiver.this.logger.debug("Starting to fetch rows");
				while ((rows = this.scanner.nextRows(HBaseRiver.this.batchSize).join()) != null) {
					if (this.stopThread) {
						HBaseRiver.this.logger.info("Stopping HBase import in the midle of it");
						break;
					}
					HBaseRiver.this.logger.debug("Processing the next {} entries in HBase parsing process", rows.size());
					final BulkRequestBuilder bulkRequest = HBaseRiver.this.esClient.prepareBulk();
					for (final ArrayList<KeyValue> row : rows) {
						if (this.stopThread) {
							HBaseRiver.this.logger.info("Stopping HBase import in the midle of it");
							break;
						}
						final IndexRequestBuilder request = HBaseRiver.this.esClient.prepareIndex(HBaseRiver.this.index,
							HBaseRiver.this.type);
						for (final KeyValue column : row) {
							request.setSource(column.key().toString(), column.value().toString());
						}
						bulkRequest.add(request);
					}
					bulkRequest.execute().addListener((ActionListener<BulkResponse>) this);
					HBaseRiver.this.logger.debug("Sent Bulk Request with HBase data to ElasticSearch");
				}
			} finally {
				stopThread();
			}
		}

		public synchronized void stopThread() {
			this.stopThread = true;
			if (this.scanner != null) {
				try {
					this.scanner.close();
				} catch (Exception e) {
					HBaseRiver.this.logger.error("An Exception has been caught while closing the HBase Scanner", e, (Object[]) null);
				}
			}
			if (this.client != null) {
				try {
					this.client.shutdown();
				} catch (Exception e) {
					HBaseRiver.this.logger.error("An Exception has been caught while shuting down the HBase client", e, (Object[]) null);
				}
			}
		}

		/**
		 * Sets the minimum time stamp on the HBase scanner, by looking into Elasticsearch for the last entry made.
		 * 
		 * @param scanner
		 */
		private void setMinTimestamp(final Scanner scanner) {
			HBaseRiver.this.logger.debug("Looking into ElasticSearch to determine timestamp of last import");
			final SearchResponse response = HBaseRiver.this.esClient.prepareSearch(HBaseRiver.this.index)
				.setTypes(HBaseRiver.this.type)
				.setQuery(QueryBuilders.matchAllQuery())
				.addFacet(FacetBuilders.statisticalFacet(TIMESTMAP_STATS).field("_timestamp"))
				.execute()
				.actionGet();

			if (response.facets().facet(TIMESTMAP_STATS) != null) {
				HBaseRiver.this.logger.debug("Got statistical data from ElasticSearch about data timestamps");
				final StatisticalFacet facet = (StatisticalFacet) response.facets().facet(TIMESTMAP_STATS);
				scanner.setMinTimestamp((long) Math.max(facet.getMax(), 0));
			}
			else {
				HBaseRiver.this.logger.debug("No statistical data about data timestamps could be found -> probably no data there yet");
				scanner.setMinTimestamp(0);
			}
		}

		/**
		 * Elasticsearch Response handler
		 */
		@Override
		public void onResponse(final BulkResponse response) {
			this.indexCounter += response.items().length;
			HBaseRiver.this.logger.info("HBase imported has indexed {} entries so far", this.indexCounter);
			if (response.hasFailures()) {
				HBaseRiver.this.logger.error("Errors have occured while trying to index new data from HBase");
			}
		}

		/**
		 * Elasticsearch Failure handler
		 */
		@Override
		public void onFailure(final Throwable e) {
			HBaseRiver.this.logger.error("An error has been caught while trying to index new data from HBase", e, new Object[] {});
		}
	}
}