package core.adapt;

import java.util.List;

import core.adapt.split.Split;
import core.index.MDIndex;
import core.utils.Pair;
import core.utils.RangeUtils.Range;
import core.utils.Schema.Attribute;

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
	
	/**
	 * A data structure to keep track of how the partitions are accessed.
	 * This information could be used to:
	 * - compute an entirely new workload-based partitioning
	 * - freeing up storage space by deleting less used block replicas
	 *  
	 */
	AccessStats stats;
	
	/**
	 * The partitioning index.
	 * 
	 */
	MDIndex partitioningIndex;
	
	
	/**
	 * Initialize hyper-partitioning data access.
	 * 
	 * This method does two things:
	 * 1. lookup the index file (if exists) for this dataset
	 * 2. de-serializes MDIndex from HDFS
	 * 
	 * @param dataset
	 */
	public void init(String dataset){
		// TODO
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
	public List<Split> getPartitionSplits(List<Predicate> predicates, int n){
		// TODO
		return null;	
	}
	
	
	public void finish(){
	}
	
}

