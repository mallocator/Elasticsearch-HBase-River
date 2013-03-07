package org.elasticsearch.river.hbase;

import java.security.InvalidParameterException;
import java.util.Map;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ExceptionsHelper;
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

public class HBaseRiver extends AbstractRiverComponent implements River {
	private final Client	esClient;
	private final String	index;
	private final String	type;
	private volatile Thread	thread;
	private boolean			stopThread;

	private final boolean	deleteOldEntries;
	private final long		interval;

	@Inject
	public HBaseRiver(RiverName riverName, RiverSettings settings, final Client esClient) {
		super(riverName, settings);
		this.esClient = esClient;
		this.logger.info("Creating MySQL Stream River");

		this.index = readConfig("index", riverName.name());
		this.type = readConfig("type", "data");
		this.deleteOldEntries = Boolean.parseBoolean(readConfig("deleteOldEntries", "true"));
		this.interval = Long.parseLong(readConfig("interval", "600000"));
	}

	@SuppressWarnings({ "unchecked" })
	private String readConfig(final String config, final String defaultValue) {
		if (this.settings.settings().containsKey("mysql")) {
			Map<String, Object> mysqlSettings = (Map<String, Object>) this.settings.settings().get("mysql");
			return XContentMapValues.nodeStringValue(mysqlSettings.get(config), defaultValue);
		}
		return defaultValue;
	}

	@Override
	public void start() {
		this.logger.info("starting hbase stream");
		try {
			this.esClient.admin()
				.indices()
				.prepareCreate(this.index)
				.addMapping(this.type, "{\"" + this.type + "\":{\"_timestamp\":{\"enabled\":true}}}")
				.execute()
				.actionGet();
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
				.setSource("{\"" + this.type + "\":{\"_timestamp\":{\"enabled\":true}}}")
				.setIgnoreConflicts(true)
				.execute()
				.actionGet();
		} catch (ElasticSearchException e) {
			this.logger.debug("Mapping already exists for index {} and type {}", this.index, this.type);
		}

		if (this.thread == null) {
			this.thread = EsExecutors.daemonThreadFactory(this.settings.globalSettings(), "hbase_slurper").newThread(new Parser());
			this.thread.start();
		}
	}

	@Override
	public void close() {
		this.logger.info("Closing HBase river");
		this.stopThread = true;
		this.thread = null;
	}

	private class Parser extends Thread {
		private Parser() {}

		@Override
		public void run() {
			HBaseRiver.this.logger.info("HBase Import Thread has started");
			long lastRun = 0;
			while (!HBaseRiver.this.stopThread) {
				if (lastRun + HBaseRiver.this.interval < System.currentTimeMillis()) {
					lastRun = System.currentTimeMillis();
					parse();
					if (HBaseRiver.this.interval <= 0) {
						break;
					}
					if (!HBaseRiver.this.stopThread) {
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

		private void parse() {
			final String timestamp = String.valueOf((int) (System.currentTimeMillis() / 1000));
			
			// TODO fetch data from Hbase according to settings
			
			if (HBaseRiver.this.deleteOldEntries) {
				HBaseRiver.this.logger.info("Removing old Hbase entries from ElasticSearch!");
				HBaseRiver.this.esClient.prepareDeleteByQuery(HBaseRiver.this.index)
					.setTypes(HBaseRiver.this.type)
					.setQuery(QueryBuilders.rangeQuery("_timestamp").lt(timestamp))
					.execute()
					.actionGet();
				HBaseRiver.this.logger.info("Old HBase entries have been removed from ElasticSearch!");
			}
			else {
				HBaseRiver.this.logger.info("Not removing old HBase entries from ElasticSearch");
			}
		}
	}
}