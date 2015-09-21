package core.access.iterator;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;

import core.access.Partition;
import core.access.Query.FilterQuery;
import core.index.robusttree.RNode;

public class DistributedRepartitionIterator extends RepartitionIterator {

	public DistributedRepartitionIterator() {
	}

	public DistributedRepartitionIterator(String iteratorString) {
		super(iteratorString);
	}

	public DistributedRepartitionIterator(FilterQuery query, RNode newIndexTree) {
		super(query, newIndexTree);
	}

	@Override
	public void finish() {

		// PartitionLock l = new PartitionLock(zookeeperHosts);

		for (Partition p : newPartitions.values()) {
			p.store(true);
		}
		System.out.println("DEBUG: oldPartitions: "
				+ Joiner.on(",").join(oldPartitions.keySet()));
		for (Partition p : oldPartitions.values()) {
			p.drop();
		}

		oldPartitions = Maps.newHashMap();
		newPartitions = Maps.newHashMap();
		// c.close();
		// l.close();
		System.gc();
	}

	// public class PartitionLock {
	//
	// private CuratorFramework client;
	// private String lockPathBase = "/partition-lock-";
	// private Map<Integer,InterProcessSemaphoreMutex> partitionLocks;
	//
	// public PartitionLock(String zookeeperHosts){
	// client = CuratorUtils.createAndStartClient(zookeeperHosts);
	// partitionLocks = Maps.newHashMap();
	// }
	//
	// public void lockPartition(int partitionId){
	// System.out.println("DEBUG: Locking partition " + partitionId);
	// partitionLocks.put(
	// partitionId,
	// CuratorUtils.acquireLock(client, lockPathBase + partitionId)
	// );
	// }
	//
	// public void unlockPartition(int partitionId){
	// if(!partitionLocks.containsKey(partitionId))
	// throw new
	// RuntimeException("Trying to unlock a partition which does not locked: "+
	// partitionId);
	//
	// CuratorUtils.releaseLock(
	// partitionLocks.get(partitionId)
	// );
	//
	// partitionLocks.remove(partitionId);
	//
	// System.out.println("DEBUG: Unlocking partition " + partitionId);
	// }
	//
	// public void close(){
	// // close any stay locks
	// for(int partitionId: partitionLocks.keySet()) {
	// System.out.println("DEBUG: Stray lock " + partitionId);
	// // unlockPartition(partitionId);
	// }
	//
	// client.close();
	// }
	//
	// public CuratorFramework getClient(){
	// return this.client;
	// }
	// }

}
