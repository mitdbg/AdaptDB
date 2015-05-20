package core.access.iterator;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessLock;

import com.google.common.collect.Maps;

import core.access.HDFSPartition;
import core.access.Partition;
import core.access.iterator.PartitionIterator.IteratorRecord;
import core.index.MDIndex.BucketCounts;
import core.index.robusttree.RNode;
import core.utils.CuratorUtils;

public class DistributedPartitioningIterator {

	protected Map<Integer,Partition> newPartitions = new HashMap<Integer,Partition>();
	protected Map<Integer,Partition> oldPartitions = new HashMap<Integer,Partition>();
	
	private Iterator<String> iterator;
	private RNode newIndexTree;
	protected String zookeeperHosts;
	
	protected Partition partition;
	protected IteratorRecord record;
	
	public DistributedPartitioningIterator(String zookeeperHosts, RNode newIndexTree){
		this.zookeeperHosts = zookeeperHosts;
		this.newIndexTree = newIndexTree;
		record = new IteratorRecord();
		partition = new HDFSPartition();	//TODO:
	}
	
	public void setIterator(Iterator<String> iterator){
		this.iterator = iterator;
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
				p = partition.clone();
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
			l.lockPartition(p.getPartitionId());
			p.store(true);
			c.addToBucketCount(p.getPartitionId(), p.getRecordCount());
			l.unlockPartition(p.getPartitionId());
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
