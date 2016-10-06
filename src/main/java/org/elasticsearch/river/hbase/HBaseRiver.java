package org.elasticsearch.river.hbase;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.admin.indices.status.ShardStatus;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.nio.charset.Charset;
import java.security.InvalidParameterException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * An HBase import river built similar to the MySQL river, that was modeled after the Solr SQL import functionality.
 */
public class HBaseRiver extends AbstractRiverComponent implements River, UncaughtExceptionHandler {


  private static final String CONFIG_SPACE = "hbase";
  private static final int MAX_TRIES = 10;
  private final Client esClient;
  private volatile Runnable parser;

  /**
   * Comma separated list of Zookeeper host to which the HBase client can connect to find the cluster.
   */
  private final String host;

  /**
   * The HBase table name to be imported from.
   */
  private final String table;

  /**
   * The ElasticSearch index name to be imported to. (default is the river name)
   */
  private final String index;

  /**
   * The ElasticSearch type name to be imported to. (Default is the source table name)
   */
  private final String type;


  /**
   * Name of the field from HBase to be used as an idField in ElasticSearch. The mapping will set up accordingly, so that
   * the _id field is routed to this field name (you can access it then under both the field name and "_id"). If no id
   * field is given, then ElasticSearch will automatically generate an id.
   */
  private final String idField;

  /**
   * The char set which is used to parse data from HBase. (Default is UTF-8)
   */
  private final Charset charset;

  /**
   * Limit the scanning of the HBase table to a certain family.
   */
  private final byte[] family;

  /**
   * Limit the scanning of the HBase table to a number of qualifiers. A family must be set for this to take effect.
   * Multiple qualifiers can be set via comma separated list.
   */
  private final String qualifiers;

  /**
   * Some names must be given in a lower case format (the index name for example), others are more flexible. This flag will
   * normalize all fields to lower case and remove special characters that ELasticSearch can't handle. (The filter is
   * probably stricter than needed in most cases)
   */
  private final boolean normalizeFields;

  /**
   * Splits up the column into further sub columns if a separator is defined. For example:
   * <p/>
   * <pre>
   * Separator: "-"
   * Columns name: "this-is-my-column"
   * Result:
   * {
   * 	this: {
   * 		is: {
   * 			my: {
   * 				column: -value-
   *      }
   *    }
   *  }
   * }
   * </pre>
   * <p/>
   * If no separator is defined, or the separator is empty, no operation is performed. Try to use single character
   * separators, as multi character separators will allow partial hits of a separator to be part of the data. (e.g. A
   * separator defined as "()" will leave all "(" and ")" in the parsed data.
   */
  public final String columnSeparator;

  /**
   * Define a custom mapping that will be used instead of an automatically generated one. Make sure to enable time stamps
   * and if you want an id-field to be recognized set the proper alias.
   */
  public final String customMapping;

  private String znode;
  private String zhosts;
  private int port;

  /**
   * Loads and verifies all the configuration needed to run this river.
   *
   * @param riverName The riverName
   * @param settings  The river settings
   * @param esClient  A client to elastic search.
   */
  @Inject
  public HBaseRiver(final RiverName riverName, final RiverSettings settings, final Client esClient) {
    super(riverName, settings);
    this.esClient = esClient;
    this.logger.info("Creating HBase Stream River");

    this.normalizeFields = Boolean.parseBoolean(readConfig("normalizeFields", "true"));
    this.host = readConfig("host");
    this.port = Integer.parseInt(readConfig("port"));
    this.znode = readConfig("znode");
    this.table = readConfig("table");
    this.zhosts = readConfig("zhosts");
    this.columnSeparator = readConfig("columnSeparator", null);
    this.idField = normalizeField(readConfig("idField", null));
    this.index = normalizeField(readConfig("index", riverName.name()));
    this.type = normalizeField(readConfig("type", this.table));
    this.charset = Charset.forName(readConfig("charset", "UTF-8"));

    final String family = readConfig("family", null);
    this.family = family != null ? family.getBytes(this.charset) : null;
    this.qualifiers = readConfig("qualifiers", null);
    this.customMapping = readConfig("customMapping", null);

  }

  /**
   * Fetch the value of a configuration that has no default value and is therefore mandatory. Empty (trimmed) strings are
   * as invalid as no value at all (null).
   *
   * @param config Key of the configuration to fetch
   * @return The value for a particular config.
   */
  private String readConfig(final String config) {
    final String result = readConfig(config, null);
    if (result == null || result.trim().isEmpty()) {
      this.logger.error("Unable to read required config {}. Aborting!", config);
      throw new InvalidParameterException("Unable to read required config " + config);
    }
    return result;
  }

  /**
   * Fetch the value of a configuration that has a default value and is therefore optional.
   *
   * @param config       Key of the configuration to fetch
   * @param defaultValue The value to set if no value could be found
   * @return The value for the config.
   */
  @SuppressWarnings({"unchecked"})
  private String readConfig(final String config, final String defaultValue) {
    if (this.settings.settings().containsKey(CONFIG_SPACE)) {
      Map<String, Object> mysqlSettings = (Map<String, Object>) this.settings.settings().get(CONFIG_SPACE);
      return XContentMapValues.nodeStringValue(mysqlSettings.get(config), defaultValue);
    }
    return defaultValue;
  }

  /**
   * This method is launched by ElasticSearch and starts the HBase River. The method will try to create a mapping with time
   * stamps enabled. If a mapping already exists the user should make sure, that time stamps are enabled for this type.
   */
  @Override
  public synchronized void start() {
    this.logger.info("Starting hbase river");

    if (this.parser != null) {
      this.logger.warn("Trying to start HBase stream although it is already running");
      return;
    }
    this.parser = new HBaseParser(this, getPort());
    this.logger.info("Waiting for Index to be ready for interaction");
    waitForESReady();
    bootStrapZookeeper(0);

    this.logger.info("Starting HBase Stream");
    try {
      String mapping = prepareCustomMapping();
      this.logger.info("Created Index {} with _timestamp mapping for {}", this.index, this.type);
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

    final Thread t = EsExecutors.daemonThreadFactory(this.settings.globalSettings(), "hbase_slurper").newThread(this.parser);
    t.setUncaughtExceptionHandler(this);
    t.start();
  }

  private String prepareCustomMapping() {
    String mapping;
    if (this.customMapping != null && !this.customMapping.trim().isEmpty()) {
      mapping = this.customMapping;
    } else {
      if (this.idField == null) {
        mapping = "{\"" + this.type + "\":{\"_timestamp\":{\"enabled\":true}}}";
      } else {
        if (this.columnSeparator != null) {
          mapping = "{\"" + this.type + "\":{\"_timestamp\":{\"enabled\":true},\"_id\":{\"path\":\""
              + this.idField.replace(this.columnSeparator, ".") + "\"}}}";
        } else {
          mapping = "{\"" + this.type + "\":{\"_timestamp\":{\"enabled\":true},\"_id\":{\"path\":\"" + this.idField + "\"}}}";
        }
      }
    }
    this.esClient.admin().indices().prepareCreate(this.index).addMapping(this.type, mapping).execute().actionGet();
    return mapping;
  }

  private void cleanupOldZookeeper(final ZooKeeper zk, String parent) throws KeeperException, InterruptedException {
    getLogger().info("Cleaning up old zookeeper: " + parent);
    List<String> children = zk.getChildren(parent, false);
    for (String child : children) {
      cleanupOldZookeeper(zk, parent + "/" + child);
    }
    Stat stat = zk.exists(parent, false);
    zk.delete(parent, stat.getVersion());
  }

  private void bootStrapZookeeper(final int attempt) {
    if (attempt > MAX_TRIES) {
      return;
    }

    try {
      ZooKeeper zooKeeper = new ZooKeeper(getZHosts(),
          300,
          (Watcher) this.parser);
      Stat stat = zooKeeper.exists(getZNode(), false);
      UUID uuid = UUID.randomUUID();
      String HBASE_ID = "hbaseid";
      String RS = "rs";
      String MAGIC_ID = "1374084687099";

      byte[] uuidBytes = Bytes.toBytes(uuid.toString());

      String tmp_znode;
      if (!getZNode().startsWith("/")) {
        tmp_znode = "/" + getZNode();
      } else {
        tmp_znode = getZNode();
      }
      if (stat != null) {
        cleanupOldZookeeper(zooKeeper, tmp_znode);
      }
      zooKeeper.create(getZNode(), new byte[0],
          ZooDefs.Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT);

      zooKeeper.create(getZNode() + "/" + HBASE_ID,
          uuidBytes,
          ZooDefs.Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT);
      zooKeeper.create(getZNode() + "/" + RS,
          new byte[0],
          ZooDefs.Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT);
      zooKeeper.create(getZNode() + "/" + RS + "/" + getHost() + ","
          + getPort()
          + "," + MAGIC_ID,
          new byte[0],
          ZooDefs.Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT);
    } catch (IOException e) {
      logger.error(e.getMessage());
    } catch (InterruptedException e) {
      logger.error(e.getMessage());
    } catch (KeeperException e) {
      bootStrapZookeeper(attempt + 1);
      logger.error(e.getMessage());
    }
  }

  private void waitForESReady() {
    if (!this.esClient.admin().indices().prepareExists(this.index).execute().actionGet().isExists()) {
      return;
    }
    for (final ShardStatus status : this.esClient.admin().indices().prepareStatus(this.index).execute().actionGet().getShards()) {
      if (status.getState() != IndexShardState.STARTED) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          this.logger.trace("HBase thread has been interrupted while waiting for the database to be reachable");
        }
        this.logger.trace("Waiting...");
        waitForESReady();
        break;
      }
    }
  }

  /**
   * This method is called by ElasticSearch when shutting down the river. The method will stop the thread and close all
   * connections to HBase.
   */
  @Override
  public synchronized void close() {
    this.logger.info("Closing HBase river");
    this.parser = null;
  }

  /**
   * Some of the asynchronous methods of the HBase client will throw Exceptions that are not caught anywhere else.
   */
  @Override
  public void uncaughtException(final Thread arg0, final Throwable arg1) {
    this.logger.error("An Exception has been thrown in HBase Import Thread", arg1, (Object[]) null);
  }

  /**
   * If the normalizeField flag is set, this method will return a lower case representation of the field, as well as
   * stripping away all special characters except "-" and "_".
   *
   * @param fieldName The fieldname to be normalized
   * @return The normalized fieldname.
   */
  public String normalizeField(final String fieldName) {
    if (!isNormalizeFields() || fieldName == null) {
      return fieldName;
    }
    if (getColumnSeparator() != null) {
      String regex = "a-z0-9\\-_";
      for (int i = 0; i < getColumnSeparator().length(); i++) {
        regex += "\\" + getColumnSeparator().charAt(i);
      }
      return fieldName.toLowerCase().replaceAll("[^" + regex + "]", "");
    }
    return fieldName.toLowerCase().replaceAll("[^a-z0-9\\-_]", "");
  }

  public boolean isNormalizeFields() {
    return this.normalizeFields;
  }

  @SuppressWarnings("unused")
  public String getTable() {
    return this.table;
  }

  public String getHost() {
    return this.host;
  }

  public String getZHosts() {
    return this.zhosts;
  }

  public String getZNode() {
    return this.znode;
  }

  @SuppressWarnings("unused")
  public byte[] getFamily() {
    return this.family;
  }

  @SuppressWarnings("unused")
  public String getQualifiers() {
    return this.qualifiers;
  }

  public Charset getCharset() {
    return this.charset;
  }

  public Client getEsClient() {
    return this.esClient;
  }

  public String getIndex() {
    return this.index;
  }

  public String getType() {
    return this.type;
  }

  public String getIdField() {
    return this.idField;
  }

  public String getColumnSeparator() {
    return this.columnSeparator;
  }

  public ESLogger getLogger() {
    return this.logger;
  }

  public int getPort() {
    return port;
  }
}
