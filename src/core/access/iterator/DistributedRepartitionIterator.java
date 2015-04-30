package core.access.iterator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessLock;

import com.google.common.collect.Maps;

import core.access.Partition;
import core.index.MDIndex.BucketCounts;
import core.utils.CuratorUtils;

public class DistributedRepartitionIterator extends RepartitionIterator {

	private String zookeeperHosts;
	
	public DistributedRepartitionIterator() {
	}
	
	public DistributedRepartitionIterator(RepartitionIterator itr, String zookeeperHosts){
		super(itr.getQuery(), itr.getIndexTree());
		this.zookeeperHosts = zookeeperHosts;
	}
	
	protected void finalize(){
		PartitionLock l = new PartitionLock(zookeeperHosts);
		BucketCounts c = new BucketCounts(l.getClient());
		
		for(Partition p: newPartitions.values()){
			l.lockPartition(p.getPartitionId());
			p.store(true);			
			c.setBucketCount(p.getPartitionId(), p.getRecordCount());
			l.unlockPartition(p.getPartitionId());
		}
		partition.drop();
		
		c.close();
		l.close();
	}
	
	public void write(DataOutput out) throws IOException{
		super.write(out);
		out.writeBytes(zookeeperHosts+"\n");
	}
	
	public void readFields(DataInput in) throws IOException{
		super.readFields(in);
		this.zookeeperHosts = in.readLine();
	}
	
	
	
	
	
	public class PartitionLock {
		
		private CuratorFramework client;		
		private String lockPathBase = "partition-lock-";
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
