package core.access;


import core.access.iterator.PartitionIterator;
import core.adapt.opt.Optimizer;
/**
 * This access method class considers filter access method over the distributed dataset.
 * The filter could be extracted as:
 * - the selection predicate in selection query
 * - the sub-range filter (different for each node) in join/aggregate query
 *
 * Currently, we support filtering only on one attribute at a time, i.e.
 * we expect the query processor to filter on the most selective attribute.
 *
 * Filter query:
 *  - can access only the local blocks on each node
 *  - scan over partitioned portion
 *  - crack over non-partitioned portion
 *
 */

public class AccessMethod {
	Optimizer opt;

	/**
	 * Initialize hyper-partitioning data access.
	 *
	 * This method does two things:
	 * 1. lookup the index file (if exists) for this dataset
	 * 2. de-serializes MDIndex from HDFS
	 *
	 * @param dataset
	 */
	public void init(String dataset, String hadoopHome){
		opt = new Optimizer(dataset);
		opt.loadIndex(hadoopHome);
	}

	/**
	 * This method returns whether or not a given partition qualifies for the predicate.
	 *
	 * @param partition
	 * @param predicate
	 * @return
	 */
	public boolean isRelevant(String partitionid, Predicate predicate){
		return true;
	}

	/**
	 * This method is used to:
	 * 1. lookup the partition index for relevant partitions
	 * 2. and, to create splits of partitions which could be assigned to different node.
	 *
	 * The split thus produced must be:
	 * (a) equal in size
	 * (b) contain blocks from the same sub-tree
	 *
	 * @param filterPredicate - the filter predicate for data access
	 * @param n	- the number of splits to produce
	 * @return
	 */
	public PartitionSplit[] getPartitionSplits(Query q, int n, boolean justAccess){
		if (justAccess) {
			return opt.buildAccessPlan(q, n);
		} else {
			return opt.buildPlan(q, n);
		}
	}



	/**
	 * This class encapsulates a set of partitions which should be
	 * accessed in a similar fashion. Each split gets assigned to a
	 * node as a whole.
	 *
	 * @author alekh
	 *
	 */
	public static class PartitionSplit {
		private int[] partitionIds;
		private PartitionIterator iterator;

		public PartitionSplit(int[] partitionIds, PartitionIterator iterator){
			this.partitionIds = partitionIds;
			this.iterator = iterator;
		}

		public int[] getPartitions(){
			return this.partitionIds;
		}

		public PartitionIterator getIterator(){
			return this.iterator;
		}
	}


	public void finish(){
	}

}

