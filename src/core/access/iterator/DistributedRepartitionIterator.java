package core.access.iterator;

import java.util.Map;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessLock;

import com.google.common.collect.Maps;

import core.access.Partition;
import core.access.Query.FilterQuery;
import core.index.MDIndex.BucketCounts;
import core.index.robusttree.RNode;
import core.utils.CuratorUtils;

public class DistributedRepartitionIterator extends RepartitionIterator {

	//private static final long serialVersionUID = 1L;
	
	public DistributedRepartitionIterator() {
	}

	public DistributedRepartitionIterator(String iteratorString){
		super(iteratorString);
	}
	
	public DistributedRepartitionIterator(FilterQuery query, RNode newIndexTree){
		super(query, newIndexTree);
	}

	@Override
	public void finish(){
		PartitionLock l = new PartitionLock(zookeeperHosts);
		BucketCounts c = new BucketCounts(l.getClient());

		for(Partition p: newPartitions.values()){
			l.lockPartition(p.getPartitionId());
			p.store(true);
			c.addToBucketCount(p.getPartitionId(), p.getRecordCount());
			l.unlockPartition(p.getPartitionId());
		}
		partition.drop();

		c.removeBucketCount(partition.getPartitionId());
		c.close();
		l.close();
	}

//	public void write(DataOutput out) throws IOException{
//		super.write(out);
//		out.writeBytes(zookeeperHosts+"\n");
//	}
//
//	public void readFields(DataInput in) throws IOException{
//		super.readFields(in);
//		this.zookeeperHosts = in.readLine();
//	}





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
