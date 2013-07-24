package org.elasticsearch.river.hbase;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.MultiAction;
import org.apache.hadoop.hbase.client.MultiResponse;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Exec;
import org.apache.hadoop.hbase.client.coprocessor.ExecResult;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.io.hfile.BlockCacheColumnFamilySummary;
import org.apache.hadoop.hbase.regionserver.CompactionRequestor;
import org.apache.hadoop.hbase.regionserver.FlushRequester;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Leases;
import org.apache.hadoop.hbase.regionserver.RegionOpeningState;
import org.apache.hadoop.hbase.regionserver.RegionServerAccounting;
import org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.net.ConnectException;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: alex
 * Date: 7/22/13
 * Time: 4:03 PM
 * To change this template use File | Settings | File Templates.
 */
public class UnimplementedInHRegionShim {


  public CatalogTracker getCatalogTracker() {
    throw new RuntimeException("Not implemented");
  }


  public ServerName getServerName() {
    throw new RuntimeException("Not implemented");
  }


  public boolean checkOOME(Throwable e) {
    throw new RuntimeException("Not implemented");
  }


  public HRegionInfo getRegionInfo(byte[] regionName) throws NotServingRegionException, ConnectException, IOException {
    throw new RuntimeException("Not implemented");
  }


  public void flushRegion(byte[] regionName) throws IllegalArgumentException, IOException {
    throw new RuntimeException("Not implemented");
  }


  public void flushRegion(byte[] regionName, long ifOlderThanTS) throws IllegalArgumentException, IOException {
    throw new RuntimeException("Not implemented");
  }


  public long getLastFlushTime(byte[] regionName) {
    throw new RuntimeException("Not implemented");
  }


  public List<String> getStoreFileList(byte[] regionName, byte[] columnFamily) throws IllegalArgumentException {
    throw new RuntimeException("Not implemented");
  }


  public List<String> getStoreFileList(byte[] regionName, byte[][] columnFamilies) throws IllegalArgumentException {
    throw new RuntimeException("Not implemented");
  }


  public List<String> getStoreFileList(byte[] regionName) throws IllegalArgumentException {
    throw new RuntimeException("Not implemented");
  }


  public Result getClosestRowBefore(byte[] regionName, byte[] row, byte[] family) throws IOException {
    throw new RuntimeException("Not implemented");
  }


  public Result get(byte[] regionName, Get get) throws IOException {
    throw new RuntimeException("Not implemented");
  }


  public boolean exists(byte[] regionName, Get get) throws IOException {
    throw new RuntimeException("Not implemented");
  }


  public void put(byte[] regionName, Put put) throws IOException {
    throw new RuntimeException("Not implemented");
  }


  public int put(byte[] regionName, List<Put> puts) throws IOException {
    throw new RuntimeException("Not implemented");
  }


  public void delete(byte[] regionName, Delete delete) throws IOException {
    throw new RuntimeException("Not implemented");
  }


  public int delete(byte[] regionName, List<Delete> deletes) throws IOException {
    throw new RuntimeException("Not implemented");
  }


  public boolean checkAndPut(byte[] regionName, byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put) throws IOException {
    throw new RuntimeException("Not implemented");
  }


  public boolean checkAndDelete(byte[] regionName, byte[] row, byte[] family, byte[] qualifier, byte[] value, Delete delete) throws IOException {
    throw new RuntimeException("Not implemented");
  }


  public long incrementColumnValue(byte[] regionName, byte[] row, byte[] family, byte[] qualifier, long amount, boolean writeToWAL) throws IOException {
    throw new RuntimeException("Not implemented");
  }


  public void mutateRow(byte[] regionName, RowMutations rm) throws IOException {
    throw new RuntimeException("Not implemented");
  }


  public Result append(byte[] regionName, Append append) throws IOException {
    throw new RuntimeException("Not implemented");
  }


  public Result increment(byte[] regionName, Increment increment) throws IOException {
    throw new RuntimeException("Not implemented");
  }


  public long openScanner(byte[] regionName, Scan scan) throws IOException {
    throw new RuntimeException("Not implemented");
  }


  public Result next(long scannerId) throws IOException {
    throw new RuntimeException("Not implemented");
  }


  public Result[] next(long scannerId, int numberOfRows) throws IOException {
    throw new RuntimeException("Not implemented");
  }


  public void close(long scannerId) throws IOException {
    throw new RuntimeException("Not implemented");
  }


  public long lockRow(byte[] regionName, byte[] row) throws IOException {
    throw new RuntimeException("Not implemented");
  }


  public void unlockRow(byte[] regionName, long lockId) throws IOException {
    throw new RuntimeException("Not implemented");
  }


  public List<HRegionInfo> getOnlineRegions() throws IOException {
    throw new RuntimeException("Not implemented");
  }


  public HServerInfo getHServerInfo() throws IOException {
    throw new RuntimeException("Not implemented");
  }


  public <R> MultiResponse multi(MultiAction<R> multi) throws IOException {
    throw new RuntimeException("Not implemented");
  }


  public boolean bulkLoadHFiles(List<Pair<byte[], String>> familyPaths, byte[] regionName) throws IOException {
    throw new RuntimeException("Not implemented");
  }


  public RegionOpeningState openRegion(HRegionInfo region) throws IOException {
    throw new RuntimeException("Not implemented");
  }


  public RegionOpeningState openRegion(HRegionInfo region, int versionOfOfflineNode) throws IOException {
    throw new RuntimeException("Not implemented");
  }


  public void openRegions(List<HRegionInfo> regions) throws IOException {
    throw new RuntimeException("Not implemented");
  }


  public boolean closeRegion(HRegionInfo region) throws IOException {
    throw new RuntimeException("Not implemented");
  }


  public boolean closeRegion(HRegionInfo region, int versionOfClosingNode) throws IOException {
    throw new RuntimeException("Not implemented");
  }


  public boolean closeRegion(HRegionInfo region, boolean zk) throws IOException {
    throw new RuntimeException("Not implemented");
  }


  public boolean closeRegion(byte[] encodedRegionName, boolean zk) throws IOException {
    throw new RuntimeException("Not implemented");
  }


  public void flushRegion(HRegionInfo regionInfo) throws NotServingRegionException, IOException {
    throw new RuntimeException("Not implemented");
  }


  public void splitRegion(HRegionInfo regionInfo) throws NotServingRegionException, IOException {
    throw new RuntimeException("Not implemented");
  }


  public void splitRegion(HRegionInfo regionInfo, byte[] splitPoint) throws NotServingRegionException, IOException {
    throw new RuntimeException("Not implemented");
  }


  public void compactRegion(HRegionInfo regionInfo, boolean major) throws NotServingRegionException, IOException {
    throw new RuntimeException("Not implemented");
  }


  public void compactRegion(HRegionInfo regionInfo, boolean major, byte[] columnFamily) throws NotServingRegionException, IOException {
    throw new RuntimeException("Not implemented");
  }

  public ExecResult execCoprocessor(byte[] regionName, Exec call) throws IOException {
    throw new RuntimeException("Not implemented");
  }


  public boolean checkAndPut(byte[] regionName, byte[] row, byte[] family, byte[] qualifier, CompareFilter.CompareOp compareOp, WritableByteArrayComparable comparator, Put put) throws IOException {
    throw new RuntimeException("Not implemented");
  }


  public boolean checkAndDelete(byte[] regionName, byte[] row, byte[] family, byte[] qualifier, CompareFilter.CompareOp compareOp, WritableByteArrayComparable comparator, Delete delete) throws IOException {
    throw new RuntimeException("Not implemented");
  }


  public List<BlockCacheColumnFamilySummary> getBlockCacheColumnFamilySummaries() throws IOException {
    throw new RuntimeException("Not implemented");
  }


  public byte[][] rollHLogWriter() throws IOException, FailedLogCloseException {
    throw new RuntimeException("Not implemented");
  }


  public String getCompactionState(byte[] regionName) throws IOException {
    throw new RuntimeException("Not implemented");
  }

  public HLog getWAL() {
    throw new RuntimeException("Not implemented");
  }


  public CompactionRequestor getCompactionRequester() {
    throw new RuntimeException("Not implemented");
  }


  public FlushRequester getFlushRequester() {
    throw new RuntimeException("Not implemented");
  }


  public RegionServerAccounting getRegionServerAccounting() {
    throw new RuntimeException("Not implemented");
  }


  public void postOpenDeployTasks(HRegion r, CatalogTracker ct, boolean daughter) throws KeeperException, IOException {
    throw new RuntimeException("Not implemented");
  }


  public boolean removeFromRegionsInTransition(HRegionInfo hri) {
    throw new RuntimeException("Not implemented");
  }


  public boolean containsKeyInRegionsInTransition(HRegionInfo hri) {
    throw new RuntimeException("Not implemented");
  }


  public FileSystem getFileSystem() {
    throw new RuntimeException("Not implemented");
  }


  public Leases getLeases() {
    throw new RuntimeException("Not implemented");
  }


  public void addToOnlineRegions(HRegion r) {
    throw new RuntimeException("Not implemented");
  }


  public boolean removeFromOnlineRegions(String encodedRegionName) {
    throw new RuntimeException("Not implemented");
  }


  public HRegion getFromOnlineRegions(String encodedRegionName) {
    throw new RuntimeException("Not implemented");
  }


  public List<HRegion> getOnlineRegions(byte[] tableName) throws IOException {
    throw new RuntimeException("Not implemented");
  }

}
