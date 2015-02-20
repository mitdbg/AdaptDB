package core.adapt.partition;

import java.util.Set;

import com.google.common.collect.Sets;

public class DirtyPartitionBuffer {

	private Set<Partition> dirtyPartitions;
	private int dirtyThreshold;
	
	@SuppressWarnings("unused")
	private int mergeThreshold;
	@SuppressWarnings("unused")
	private int evictThreshold;
	
	public DirtyPartitionBuffer(){
		dirtyPartitions = Sets.newHashSet();
		dirtyThreshold = 1000;	// no threshold
	}
	
	public DirtyPartitionBuffer(int dirtyThreshold){
		dirtyPartitions = Sets.newHashSet();
		this.dirtyThreshold = dirtyThreshold;
	}
	
	public void add(Partition p){
		if(dirtyPartitions.size() > dirtyThreshold){
			// TODO: merge()
			// TODO: evict()
		}
		dirtyPartitions.add(p);
	}

	/**
	 * This method merges dirty partitions (removes some partitions, adds some partitions)
	 * 
	 * @param unpartitionAttribute -- the attribute to unpartition on
	 * @param count -- the number of partitions to merge
	 * 
	 */
	public void merge(Object unpartitionAttribute, int count){
		// TODO:
	}
	
	public void mergeAll(Object unpartitionAttribute){
		merge(unpartitionAttribute, dirtyPartitions.size());
	}
	
	/**
	 * This method evicts some partitions.
	 * Removes the evicted partitions from the dirtyPartitions set.
	 * 
	 * @param count -- the number of partitions to evict
	 */
	public void evict(int count){
		// TODO:
	}
	
	public void evictAll(){
		evict(dirtyPartitions.size());
	}
}
