package core.adapt.partition.merger;

import java.util.List;

import com.google.common.collect.Lists;

import core.access.Partition;

public abstract class PartitionMerger {

	private int mergeAttrIdx;
	protected Partition currentPartition;
	
	public PartitionMerger(int mergeAttrIdx){
		this.mergeAttrIdx = mergeAttrIdx;
	}
	
	public Partition[] getMergedPartitions(Partition...partitions){
		List<Partition> mergedPartitions = Lists.newArrayList();
		for(Partition p: partitions)
			mergedPartitions.add(getMergedPartition(p));
		return (Partition[])mergedPartitions.toArray();
				
		//TODO: re-balance the partitions in case of very low/high selectivity
		// TODO: repartitioning is futile if the selected portion is very low selectivity (almost all)
	}
	
	protected Partition getMergedPartition(Partition p){
		currentPartition = p; 
		p.lineage[mergeAttrIdx] = mapMerge(p.lineage[mergeAttrIdx]);
		return p;
	}
	
	// return new id
	protected abstract int mapMerge(int oldId);
	

	/**
	 * Merge every k partitions.
	 */
	public static class KWayMerge extends PartitionMerger{
		private int k;
		public KWayMerge(int mergeAttrIdx, int k) {
			super(mergeAttrIdx);
			this.k = k;
		}
		protected int mapMerge(int oldId) {
			return oldId / k;
		}
	}
	
	/**
	 * Merge partitions s.t. the size after merging is as close to maxSize as possible. 
	 */
	public static class BalancedMerge extends PartitionMerger{
		private int maxSize;
		public BalancedMerge(int mergeAttrIdx, int maxSize) {
			super(mergeAttrIdx);
			this.maxSize = maxSize;
		}
		protected int mapMerge(int oldId) {
			int k = maxSize / currentPartition.getSize();
			return oldId / k;
		}
	}
	
	//TODO: need a merger which sorts each merged partition (in order to create balanced partitions)
}
