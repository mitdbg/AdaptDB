package core.access.iterator;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessLock;

import com.google.common.collect.Maps;

import core.access.Partition;
import core.index.MDIndex.BucketCounts;
import core.index.robusttree.RNode;
import core.utils.CuratorUtils;

public class DistributedPartitioningIterator implements Serializable {

	private static final long serialVersionUID = 1L;

	protected Map<Integer,Partition> newPartitions = new HashMap<Integer,Partition>();
	protected Map<Integer,Partition> oldPartitions = new HashMap<Integer,Partition>();
	
	private Iterator<String> iterator;
	private transient RNode newIndexTree;
	protected String zookeeperHosts;
	private String propertiesFile;
	private String hdfsPath;
	
	protected Partition partition;
	protected IteratorRecord record;
	
	public DistributedPartitioningIterator(String zookeeperHosts, RNode newIndexTree, String propertiesFile, String path){
		this.zookeeperHosts = zookeeperHosts;
		this.newIndexTree = newIndexTree;
		record = new IteratorRecord();
		this.propertiesFile = propertiesFile;
		this.hdfsPath = path;
	}
	
	public void setIterator(Iterator<String> iterator){
		this.iterator = iterator;
	}

	public void setPartition(Partition p) {
		partition = p;
	}

	public boolean hasNext() {
		
		if(iterator.hasNext()){
			record.setBytes(iterator.next().getBytes());

			int id = newIndexTree.getBucketId(record);
			Partition p;
			if(newPartitions.containsKey(id)){
				p = newPartitions.get(id);
			}
			else{
				p = partition.getHDFSClone(propertiesFile, hdfsPath+"/0");
				//p = partition.clone();
				p.setPartitionId(id);
				newPartitions.put(id, p);
			}
			// p.write(record.getBytes(), record.getOffset(), record.getLength());
			p.write(record.getBytes(), 0, record.getBytes().length);
			
			return true;
		}
		
		return false;
	}
	
	protected boolean isRelevant(IteratorRecord record){
		return true;
	}
	
	public void finish(){
		PartitionLock l = new PartitionLock(zookeeperHosts);
		BucketCounts c = new BucketCounts(l.getClient());

		for(Partition p: newPartitions.values()){
			int id = p.getPartitionId();
			l.lockPartition(id);
			p.store(true);
			c.addToBucketCount(p.getPartitionId(), p.getRecordCount());
			l.unlockPartition(id);
		}
		for(Partition p: oldPartitions.values()){
			p.drop();
			c.removeBucketCount(p.getPartitionId());
		}		
		oldPartitions = Maps.newHashMap();
		newPartitions = Maps.newHashMap();
		c.close();
		l.close();
	}

	private void writeObject(java.io.ObjectOutputStream out) throws IOException {
		out.defaultWriteObject();
		out.writeBytes(newIndexTree.marshall());
	}

	private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
		in.defaultReadObject();
		byte[] bytes = new byte[1024*1024*10]; // is this big enough?
		in.read(bytes);
		newIndexTree = new RNode();
		newIndexTree.unmarshall(bytes);
	}



	public class PartitionLock {

		private CuratorFramework client;
		private String lockPathBase = "/partition-lock-";
		private Map<Integer,InterProcessLock> partitionLocks;


		public PartitionLock(String zookeeperHosts){
			client = CuratorUtils.createAndStartClient(zookeeperHosts);
			partitionLocks = Maps.newHashMap();
		}

		public void lockPartition(int partitionId){
			partitionLocks.put(
					partitionId,
					CuratorUtils.acquireLock(client, lockPathBase + partitionId)
				);
		}

		public void unlockPartition(int partitionId){
			if(!partitionLocks.containsKey(partitionId))
				throw new RuntimeException("Trying to unlock a partition which does not locked: "+ partitionId);

			CuratorUtils.releaseLock(
					partitionLocks.get(partitionId)
				);
			partitionLocks.remove(partitionId);
		}

		public void close(){
			// close any stay locks
			for(int partitionId: partitionLocks.keySet())
	 			unlockPartition(partitionId);
			client.close();
		}

		public CuratorFramework getClient(){
			return this.client;
		}
	}

}
