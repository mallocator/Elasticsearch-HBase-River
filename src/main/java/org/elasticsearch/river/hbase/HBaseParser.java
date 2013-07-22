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
import org.apache.hadoop.hbase.ipc.RpcEngine;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
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
import java.util.UUID;

/**
 * A separate Thread that does the actual fetching and storing of data from an HBase cluster.
 *
 * @author Ravi Gairola
 */
class HBaseParser extends UnimplementedInHRegionShim
    implements Watcher,
    HRegionInterface,
    HBaseRPCErrorHandler,
    Runnable,
    RegionServerServices {

  private final InetSocketAddress initialIsa;
  private final RpcServer rpcServer;
  private final RpcEngine rpcEngine;
  private HBaseServer server;
  ZooKeeper zooKeeper;
  Configuration c;

  final static int PORT_NUMBER = 8080;
  final static String hostname = "localhost";
  private static final String TIMESTMAP_STATS = "timestamp_stats";
  private final HBaseRiver river;
  private final ESLogger logger;
  private int indexCounter;
  private boolean stopThread;

  HBaseParser(final HBaseRiver river) throws IOException {
    this.river = river;
    this.logger = river.getLogger();
    zooKeeper = new ZooKeeper("localhost:2181", 300, this);
    c = HBaseConfiguration.create();

    initialIsa = new InetSocketAddress(PORT_NUMBER);
    int numHandler = 10;
    int metaHandlerCount = 10;
    boolean verbose = true;
    this.rpcServer = HBaseRPC.getServer(this,
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
    if (rpcServer instanceof HBaseServer) server = (HBaseServer) rpcServer;

    this.rpcServer.setErrorHandler(this);
    this.rpcEngine = HBaseRPC.getProtocolEngine(c);
    this.rpcServer.start();
  }

  /**
   * Timing mechanism of the thread that determines when a parse operation is supposed to run. Waits for the predefined
   * interval until a new run is performed. The method checks every 1000ms if it should be parsing again. The first run is
   * done immediately once the thread is started.
   */
  @Override
  public void run() {
    Stat stat = new Stat();
    byte[] hostnameBytes = Bytes.toBytes(hostname);
    byte[] hostnameLengthBytes = Bytes.toBytes(new Long(hostname.length()));
    UUID uuid = UUID.randomUUID();
    byte[] uuidBytes = Bytes.toBytes(uuid.toString());

    try {
      stat = this.zooKeeper.exists("/hbase.o", false);
      if (stat == null) {

        this.zooKeeper.create("/hbase.o", new byte[0],
            ZooDefs.Ids.OPEN_ACL_UNSAFE,
            CreateMode.PERSISTENT);

        this.zooKeeper.create("/hbase.o/hbaseid",
            uuidBytes,
            ZooDefs.Ids.OPEN_ACL_UNSAFE,
            CreateMode.PERSISTENT);
        this.zooKeeper.create("/hbase.o/rs",
            new byte[0],
            ZooDefs.Ids.OPEN_ACL_UNSAFE,
            CreateMode.PERSISTENT);
        this.zooKeeper.create("/hbase.o/rs/" + hostname + ","
            + PORT_NUMBER
            + ",1374084687099",
            new byte[0],
            ZooDefs.Ids.OPEN_ACL_UNSAFE,
            CreateMode.PERSISTENT);


        byte[] data = this.zooKeeper.getData("/hbase/hbaseid", false, stat);
        byte[] data2 = this.zooKeeper.getData("/hbase.o/hbaseid", false, stat);
      }
    } catch (KeeperException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    } catch (InterruptedException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }

    this.logger.info("HBase Import Thread has started");
    long lastRun = 0;
    while (!this.stopThread) {
      if (lastRun + this.river.getInterval() < System.currentTimeMillis()) {
        lastRun = System.currentTimeMillis();
        try {
          this.indexCounter = 0;
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
   * Checks if there is an open Scanner or Client and closes them.
   */
  public synchronized void stopThread() {
    this.stopThread = true;
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
    this.logger.info("fooo");
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
   * @param row
   * @return
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
