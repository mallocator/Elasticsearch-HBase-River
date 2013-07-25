package org.elasticsearch.river.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ipc.HBaseRPC;
import org.apache.hadoop.hbase.ipc.HBaseRPCErrorHandler;
import org.apache.hadoop.hbase.ipc.HBaseServer;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.ipc.ProtocolSignature;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.logging.ESLogger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A separate Thread that provides a replication sink and stores that data
 * from an HBase cluster.
 */
class HBaseParser extends UnimplementedInHRegionShim
    implements Watcher,
    HRegionInterface,
    HBaseRPCErrorHandler,
    Runnable,
    RegionServerServices {

  private final InetSocketAddress initialIsa;
  private HBaseServer server;
  Configuration c;

  private final HBaseRiver river;
  private final ESLogger logger;
  private int indexCounter;
  int numHandler;
  int metaHandlerCount;
  boolean verbose;

  HBaseParser(final HBaseRiver river, int port_number) {
    this.river = river;
    this.logger = river.getLogger();
    initialIsa = new InetSocketAddress(port_number);
    numHandler = 10;
    metaHandlerCount = 10;
    verbose = true;
    c = HBaseConfiguration.create();
  }

  @Override
  public void run() {
    try {
      c = HBaseConfiguration.create();
      RpcServer rpcServer = HBaseRPC.getServer(this,
          new Class<?>[]{
              HBaseRPCErrorHandler.class
          },
          initialIsa.getHostName(), // BindAddress is IP we got for this server.
          initialIsa.getPort(),
          numHandler,
          metaHandlerCount,
          verbose,
          c,
          HConstants.QOS_THRESHOLD);
      rpcServer.setErrorHandler(this);
      if (rpcServer instanceof HBaseServer) server = (HBaseServer) rpcServer;
      rpcServer.start();
    } catch (IOException e) {
      this.logger.error("Unable to start RPCServer");
    }
  }

  @Override
  public void abort(String why, Throwable e) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public boolean isAborted() {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public ProtocolSignature getProtocolSignature(
      String protocol, long version, int clientMethodsHashCode)
      throws IOException {
    if (protocol.equals(HRegionInterface.class.getName())) {
      return new ProtocolSignature(HRegionInterface.VERSION, null);
    }
    throw new IOException("Unknown protocol: " + protocol);
  }

  @Override
  public long getProtocolVersion(final String protocol, final long clientVersion)
      throws IOException {
    if (protocol.equals(HRegionInterface.class.getName())) {
      return HRegionInterface.VERSION;
    }
    throw new IOException("Unknown protocol: " + protocol);
  }

  @Override
  public void process(WatchedEvent watchedEvent) {
  }

  @Override
  public RpcServer getRpcServer() {
    return server;
  }

  @Override
  public void stop(String why) {
    throw new RuntimeException("Not implemented");
  }


  @Override
  public boolean isStopped() {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public boolean isStopping() {
    throw new RuntimeException("Not implemented");
  }

  public Configuration getConfiguration() {
    return c;
  }


  public ZooKeeperWatcher getZooKeeper() {
    throw new RuntimeException("Not implemented");
  }


  public void replicateLogEntries(HLog.Entry[] entries) throws IOException {
    final BulkRequestBuilder bulkRequest = this.river.getEsClient().prepareBulk();

    for (HLog.Entry entry : entries) {
      final IndexRequestBuilder request = this.
          river.
          getEsClient().
          prepareIndex(this.river.getIndex(), this.river.getType());
      List<KeyValue> kvs = entry.getEdit().getKeyValues();
      if (kvs.size() > 0) {
        final byte[] key = kvs.get(0).getRow();
        Map<String, Object> dataTree = readDataTree(kvs);
        request.setSource(dataTree);
        request.setTimestamp(String.valueOf(kvs.get(0).getTimestamp()));
        if (this.river.getIdField() == null) {
          final String keyString = new String(key, this.river.getCharset());
          request.setId(keyString);
        }
        bulkRequest.add(request);
      }
    }
    final BulkResponse response = bulkRequest.execute().actionGet();
    this.indexCounter += response.getItems().length;
    this.logger.info("HBase river has indexed {} entries so far", this.indexCounter);
    final List<byte[]> failedKeys = new ArrayList<byte[]>();
    if (response.hasFailures()) {
      this.logger.error("Errors have occured while trying to index new data from HBase");
      this.logger.debug("Failed keys are {}", failedKeys);
    }
  }

  /**
   * Generate a tree structure that ElasticSearch can read and index from one of the rows that has been returned from
   * HBase.
   *
   * @param row The row in which to generate tree data
   * @return a map representation of the HBase row
   */
  protected Map<String, Object> readDataTree(final List<KeyValue> row) {
    final Map<String, Object> dataTree = new HashMap<String, Object>();
    for (final KeyValue column : row) {
      final String family = this.river.normalizeField(new String(column.getFamily(), this.river.getCharset()));
      final String qualifier = new String(column.getQualifier(), this.river.getCharset());
      final String value = new String(column.getValue(), this.river.getCharset());
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
  protected void readQualifierStructure(final Map<String, Object> parent,
                                        final String qualifier,
                                        final String value) {
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

  protected String findKeyInDataTree(final Map<String, Object> dataTree, final String keyPath) {
    if (!keyPath.contains(this.river.getColumnSeparator())) {
      return (String) dataTree.get(keyPath);
    }
    final String key = keyPath.substring(0, keyPath.indexOf(this.river.getColumnSeparator()));
    if (dataTree.get(key) instanceof Map) {
      final int subKeyIndex = keyPath.indexOf(this.river.getColumnSeparator()) + this.river.getColumnSeparator().length();
      return findKeyInDataTree((Map<String, Object>) dataTree.get(key), keyPath.substring(subKeyIndex));
    }
    return null;
  }
}
