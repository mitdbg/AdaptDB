package core.adapt;

import java.util.List;
import java.util.Set;

import core.adapt.PartitionIterator.ITER_TYPE;
import core.adapt.Query.FilterQuery;
import core.index.MDIndex;


/*
 * We consider only filter query for adaptivity for the moment.
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
	
	Set<Partition> dirtyPartitions;
	int DIRTY_THRESHOLD = 10;
	
	/*
	 *  Initialize hyper-partitioning data access.
	 *  e.g. read the partitioning index from HDFS.
	 *  
	 */	
	public void initPartitionIndex(String indexPath){
		// de-serialize MDIndex from  HDFS
	}
	
	
	/**
	 * Get the relevant partitions for this query.
	 * @return
	 */
	public List<Partition> getRelevantPartitions(Query q){
		// 1. lookup the partition tree to find relevant partitions		
		// 2. order the partitions by their blocks such that partitions in the 
		//		same block are close to each other.		
		return null;
	}
	
	
	// process a query:
	// 	1. process partitioned portion
	// 	2. process non-partitioned portion
	//		- re-partition while processing

	public PartitionIterator iterate(Partition p, FilterQuery q, ITER_TYPE type){
		stats.collect(q);
		
		//1. too many dirty partitions?
		if(dirtyPartitions.size() > DIRTY_THRESHOLD)
			mergeAndFlush();
		
		//2. fetch
		p.loadNext();
		
		//3. produce an iterator
		switch(type){
		case SCAN:		//TODO
		case SPLIT:		//TODO
						// get the modified partitions
		case CRACK:		//TODO
						// get the modified partitions
		}
		
		return null;
	}
	
	public void mergeAndFlush(){
		// merge partitions on the same attribute
		// Two flush strategies:
		//  1. overwrite: if a partition loaded from HDFS is modified
		//	2. append: if a new partition is added to the buffer 
	}
	
	public void finalize(){
		// merge and flush periodically any remaining output partitions
		mergeAndFlush();
	}

	
	/*
	 * Join query: will need to process co-partitioned joins and standard hash joins (future work). 
	 *  
	 */
}

