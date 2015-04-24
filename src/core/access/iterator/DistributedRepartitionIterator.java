package core.access.iterator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

import com.google.common.collect.Maps;

import core.access.Partition;
import core.access.Query.FilterQuery;
import core.index.MDIndex;

public class DistributedRepartitionIterator extends RepartitionIterator {

	private String zookeeperHosts;
	
	public DistributedRepartitionIterator() {
	}
	
	public DistributedRepartitionIterator(FilterQuery query, MDIndex newIndexTree, String zookeeperHosts) {
		super(query, newIndexTree);
		this.zookeeperHosts = zookeeperHosts;
	}

	protected void finalize(){
		DistributedLock l = new DistributedLock(zookeeperHosts);
		for(Partition p: newPartitions.values()){
			l.lockPartition(p.getPartitionId());
			p.store(true);
			l.unlockPartition(p.getPartitionId());
		}
		partition.drop();
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
	
	
	
	private class DistributedLock{
		// constants
		private int baseSleepTimeMills = 1000;	
		private int maxRetries = 3;
		private int waitTimeSeconds = 1000;
		private String lockPathBase = "partition-lock-";
		
		private CuratorFramework client;
		private Map<Integer,InterProcessLock> partitionLocks;
		
		public DistributedLock(String hosts){
			//String hosts = "host-1:2181,host-2:2181,host-3:2181";
			partitionLocks = Maps.newHashMap();
			RetryPolicy retryPolicy = new ExponentialBackoffRetry(baseSleepTimeMills, maxRetries);
			client = CuratorFrameworkFactory.newClient(hosts, retryPolicy);
			client.start();
		}
		
		public void lockPartition(int partitionId){
			if(partitionLocks.containsKey(partitionId))
				throw new RuntimeException("Trying to obtain a duplicate lock on partition "+partitionId);
			
			InterProcessLock lock = new InterProcessMutex(client, lockPathBase + partitionId);			
			try {
				if (lock.acquire(waitTimeSeconds, TimeUnit.SECONDS))
					partitionLocks.put(partitionId, lock);
				else
					throw new RuntimeException("Time out: Failed to lock partition "+partitionId);
			} catch (Exception e) {
				e.printStackTrace();
				throw new RuntimeException("Failed to lock partition "+partitionId+"\n "+e.getMessage());
			}
		}
		
		public void unlockPartition(int partitionId){
			if(!partitionLocks.containsKey(partitionId))
				throw new RuntimeException("Trying to release a non-locked partition "+partitionId);
				
			try {
				partitionLocks.get(partitionId).release();
			} catch (Exception e) {
				e.printStackTrace();
				throw new RuntimeException("Failed to unlock partition "+partitionId+"\n "+e.getMessage());
			}			
			partitionLocks.remove(partitionId);
		}
		
		public void close(){
			// close any stay locks
			for(Integer partitionId: partitionLocks.keySet())
				unlockPartition(partitionId);
			client.close();
		}
	}
}
