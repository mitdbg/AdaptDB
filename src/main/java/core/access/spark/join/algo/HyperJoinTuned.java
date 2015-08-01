package core.access.spark.join.algo;

import java.util.List;

import org.apache.hadoop.mapreduce.InputSplit;

import com.google.common.collect.Lists;

import core.access.spark.join.HPJoinInput;
import core.index.MDIndex;
import core.utils.Range;

/**
 * 
 * An approximate algorithm for combining buckets in a hierarchical manner.
 * 
 * This algorithm extends the formulation in:
 * 		"Fine-grained Partitioning for Aggressive Data Skipping", SIGMOD 2014.
 * ABove paper in turn uses the Ward's method in:
 *		"Hierarchical grouping to optimize an objective function", JASA 1963.
 *
 * We extend the above algorithms by:
 * - applying grouping to buckets (and not tuples as in "Data Skipping..")
 * - considering replication of buckets
 * - more elaborate set of stopping conditions
 * 
 * @author alekh
 *
 */
public class HyperJoinTuned extends JoinAlgo {

	private HPJoinInput joinInput1;
	private HPJoinInput joinInput2;	
	private int minSplits;
	
	public HyperJoinTuned(HPJoinInput joinInput1, HPJoinInput joinInput2, int minSplits) {
		this.joinInput1 = joinInput1;
		this.joinInput2 = joinInput2;
		this.minSplits = minSplits;
	}
	
	
	/**
	 * Return the ranges units (called Rangelets here).
	 * The return set contains disjoint and complete set of ranges.
	 * Each range in the set indicates the smallest unit of data that is shuffled/replicated.
	 * 
	 * The rangelets could be obtained by:
	 * 1. simply dividing the overall join key range
	 * 2. using join key distributions
	 * 3. using samples
	 * 
	 * @param overallRange
	 * @return
	 */
	protected List<Range> getRangelets(Range overallRange){
		return null;	// TODO
	}
	
	
	/**
	 * Create the list of virtual buckets based on the range units (the rangelets).
	 * Each virtual bucket intersects with only one of the range units.
	 * The set of virtual buckets is complete over the set of input buckets.
	 * 
	 * @param buckets
	 * @param rangelets
	 * @return
	 */
	protected List<VBucket> getVirtualBuckets(List<MDIndex.BucketInfo> buckets, List<Range> rangelets){
		return null;	// TODO
	}
	
	
	/**
	 * Perform a single iteration of the algorithm. Two steps involved:
	 * 1. find the pair of partitions which result in maximum reduction in cost
	 * 2. combine the above pair (if such a pair is found)
	 * 
	 * @param partitionSet
	 * @return
	 */
	protected PartitionSet iterate(PartitionSet partitionSet){
		return null;	// TODO
	}
	
	/**
	 * The getSplit method implementation. (This method is invoked from the driver class)
	 */
	public List<InputSplit> getSplits() {
		
		// step 1: get the set of range units
		List<Range> rangelets = getRangelets(joinInput1.getFullRange());		
		
		// step 2: create virtual buckets
		List<VBucket> vbuckets = getVirtualBuckets(joinInput1.getBucketRanges(), rangelets);
		
		// step 3: initialize the partition set
		PartitionSet partitionSet = new PartitionSet(vbuckets);
		
		// step 4: the heuristic based combine step
		double initialC = partitionSet.C();
		do{
			iterate(partitionSet);
		} while(
				(initialC - partitionSet.C() > 0) &&			// (i) there is reduction in size
				(partitionSet.combineCandidates() > 0) &&		// (ii) at least one partition is combine-able
				(partitionSet.size() > minSplits)				// (iii) number of partitions greater than threshold 
			);
		
		// step 5: return the final partition set as input splits
		return partitionSet.getInputSplits();
		
	}
	

	
/*
 * 
 * The helper classes follow below.
 * 
 * 
 */

	
	/**
	 * The virtual bucket instance. 
	 * 
	 * @author alekh
	 */
	public static class VBucket{
	}
	
	/**
	 * The physical bucket instance.
	 * I guess this cane be replaced with one of the existing classes, e.g. MDIndex.BucketInfo ?
	 * 
	 * @author alekh
	 */
	public static class PBucket{
	}
	
	/**
	 * The Partition class which holds a set of virtual buckets.
	 * 
	 * @author alekh
	 */
	public static class Partition{
		/*
		 * This is true as long as:
		 * - the partition is less than the max size
		 * - the hash table size, i.e. sB, is less than the max size 
		 */
		private boolean combineCandidate = true;
		
		public Partition(VBucket vbucket){
			// TODO
		}
		public List<VBucket> v(){
			return null;
		}
		public List<PBucket> b(){
			return null;
		}
		public Range r(){
			return null;
		}
		public double sA(){
			return 0;
		}
		public double sB(){
			return 0;
		}
		public double C(){
			return 0;
		}
		public Partition combine(){
			return null;
			// TODO: update the combineCandidate flag accordingly
		}
		public boolean isCombineCandidate(){
			return combineCandidate;					
		}
	}
	
	/**
	 * An instance of a set of partitions.
	 * 
	 * @author alekh
	 */
	public static class PartitionSet {
		private List<Partition> partitions;		
		public PartitionSet(List<VBucket> vbuckets){
			partitions = Lists.newArrayList();
			for(VBucket vbucket: vbuckets)
				partitions.add(new Partition(vbucket));
		}
		public void add(Partition g){
			partitions.add(g);
		}
		public void remove(Partition g){
			partitions.remove(g);
		}
		public int size(){
			return partitions.size();
		}
		public double C(){
			double c = 0;
			for(Partition p: partitions)
				c += p.C();
			return c;
		}
		public int combineCandidates(){
			int c = 0;
			for(Partition p: partitions)
				c += p.combineCandidate ? 1:0;
			return c;
		}
		public List<InputSplit> getInputSplits(){
			return null;
		}
	}
}
