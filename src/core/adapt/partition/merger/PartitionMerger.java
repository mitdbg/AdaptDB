package core.adapt.partition.merger;

import core.adapt.partition.Partition;

public abstract class PartitionMerger {

	private int mergeAttrIdx;
	protected Partition currentPartition;
	
	public PartitionMerger(int mergeAttrIdx){
		this.mergeAttrIdx = mergeAttrIdx;
	}
	
	public Partition getMergedPartition(Partition p){
		currentPartition = p; 
		p.lineage[mergeAttrIdx] = mapMerge(p.lineage[mergeAttrIdx]);
		return p;
	}
	
	// return new id
	public abstract int mapMerge(int oldId);
	

	/**
	 * Merge every k partitions.
	 */
	public static class KWayMerge extends PartitionMerger{
		private int k;
		public KWayMerge(int mergeAttrIdx, int k) {
			super(mergeAttrIdx);
			this.k = k;
		}
		public int mapMerge(int oldId) {
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
		public int mapMerge(int oldId) {
			int k = maxSize / currentPartition.getBytes().length;
			return oldId / k;
		}
	}
}
