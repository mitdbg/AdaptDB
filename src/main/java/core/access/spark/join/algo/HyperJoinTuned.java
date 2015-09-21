package core.access.spark.join.algo;

import java.util.*;

import com.google.common.collect.*;
import core.access.AccessMethod;
import core.utils.TypeUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;

import core.access.spark.join.HPJoinInput;
import core.index.MDIndex;
import core.utils.Range;

/**
 * 
 * An approximate algorithm for combining buckets in a hierarchical manner.
 * 
 * This algorithm extends the formulation in:
 * "Fine-grained Partitioning for Aggressive Data Skipping", SIGMOD 2014. ABove
 * paper in turn uses the Ward's method in:
 * "Hierarchical grouping to optimize an objective function", JASA 1963.
 *
 * We extend the above algorithms by: - applying grouping to buckets (and not
 * tuples as in "Data Skipping..") - considering replication of buckets - more
 * elaborate set of stopping conditions
 * 
 * @author alekh
 *
 */
public class HyperJoinTuned extends JoinAlgo {

	private HPJoinInput joinInput1;
	private HPJoinInput joinInput2;

	private int minSplits;
	private double maxPartitionSize = 400000 * 1000000L;
	private double maxHashTableSize = 40000 * 1000000L;

	// private Set<PartitionPair> candidatePairs = Sets.newHashSet();
	private List<PartitionPair> candidatePairs = Lists.newArrayList();
	private Partition lastCombined = null;

	public HyperJoinTuned(HPJoinInput joinInput1, HPJoinInput joinInput2,
			int minSplits) {
		this.joinInput1 = joinInput1;
		this.joinInput2 = joinInput2;
		this.minSplits = minSplits;
	}

	public HyperJoinTuned(HPJoinInput joinInput1, HPJoinInput joinInput2) {
		this.joinInput1 = joinInput1;
		this.joinInput2 = joinInput2;
		this.minSplits = SPLIT_FANOUT;
	}

	/**
	 * Return the ranges units (called Rangelets here). The return set contains
	 * disjoint and complete set of ranges. Each range in the set indicates the
	 * smallest unit of data that is shuffled/replicated.
	 * 
	 * The rangelets could be obtained by: 1. simply dividing the overall join
	 * key range 2. using join key distributions 3. using samples
	 * 
	 * @param overallRange
	 * @return
	 */
	protected List<Range> getRangelets(Range overallRange) {
		return joinInput1.getRangeSplits(minSplits, false); // uses samples and
															// distributions
	}

	/**
	 * Create the list of virtual buckets based on the range units (the
	 * rangelets). Each virtual bucket intersects with only one of the range
	 * units. The set of virtual buckets is complete over the set of input
	 * buckets.
	 * 
	 * @param buckets
	 * @param rangelets
	 * @return
	 */
	protected List<VBucket> getVirtualBuckets(List<MDIndex.BucketInfo> buckets,
			List<Range> rangelets) {
		List<VBucket> vbuckets = new ArrayList<VBucket>();
		Range fullRange = joinInput2.getFullRange();
		for (MDIndex.BucketInfo b : buckets) {
			b.intersect(fullRange);
			PBucket p = new PBucket(b, joinInput2);
			for (Range rangelet : rangelets) {
				if (b.intersectionFraction(rangelet) > 0) {
					vbuckets.add(new VBucket(p, rangelet));
				}
			}
		}
		Collections.sort(vbuckets, new Comparator<VBucket>() {
			public int compare(VBucket o1, VBucket o2) {
				return TypeUtils.compareTo(o1.range().getLow(), o2.range()
						.getLow(), o1.range().getType());
			}
		});
		System.out.println("Initial vbuckets: " + vbuckets.size());
		return vbuckets;
	}

	/**
	 * Perform a single iteration of the algorithm. Two steps involved: 1. find
	 * the pair of partitions which result in maximum reduction in cost 2.
	 * combine the above pair (if such a pair is found)
	 * 
	 * @param partitionSet
	 * @return
	 */
	protected void iterate(PartitionSet partitionSet) {
		// create a list of all combine-able partition pairs
		// List<PartitionPair> candidatePairs; // = Lists.newArrayList();
		long start = System.currentTimeMillis();

		if (lastCombined == null) {
			// First time, consider all possible pairs
			List<Partition> partitions = new ArrayList<Partition>(
					partitionSet.getPartitions());
			Iterator<Partition> itr = partitions.iterator();
			while (itr.hasNext()) {
				Partition p1 = itr.next();
				itr.remove();
				for (Partition p2 : partitions) {
					PartitionPair partitionPair = new PartitionPair(p1, p2);
					if (!partitionPair.checkAdjacent()) {
						break;
					}
					if (partitionPair.checkCombine(maxPartitionSize,
							maxHashTableSize)) {
						candidatePairs.add(partitionPair);
					}
				}
			}
		} else {
			// Add all combinations of the lastCombined range
			for (Partition p2 : partitionSet.getPartitions()) {
				PartitionPair partitionPair = new PartitionPair(lastCombined,
						p2);
				if (!partitionPair.checkAdjacent()) {
					continue;
				}
				if (partitionPair.checkCombine(maxPartitionSize,
						maxHashTableSize)) {
					candidatePairs.add(partitionPair);
				}
			}
		}
		System.out.println("time: " + (System.currentTimeMillis() - start));

		// sort the partition pairs based on how much they can reduce the cost
		// Collections.sort(candidatePairs, new Comparator<PartitionPair>() {
		PartitionPair p = null;
		if (candidatePairs.size() > 0) {
			// TODO: could improve sort algorithm (radix?)
			Collections.sort(candidatePairs, new Comparator<PartitionPair>() {
				public int compare(PartitionPair o1, PartitionPair o2) {
					if (o1.reductionC() > o2.reductionC())
						return -1;
					else if (o1.reductionC() < o2.reductionC())
						return 1;
					else
						return 0;
				}
			});
			p = candidatePairs.get(0);

			// remove things that were marked for removal in the last round
			// (have reductionC = -1)
			PartitionPair removed = new PartitionPair(null, null);
			removed.reductionC = 0;
			int indexRemove = Collections.binarySearch(candidatePairs, removed,
					new Comparator<PartitionPair>() {
						public int compare(PartitionPair o1, PartitionPair o2) {
							if (o1.reductionC() > o2.reductionC())
								return -1;
							else if (o1.reductionC() < o2.reductionC())
								return 1;
							else
								return 0;
						}
					});
			if (indexRemove < 0) {
				indexRemove = (indexRemove + 1) * -1;
			}
			System.out.println("removing from " + indexRemove + ", "
					+ candidatePairs.size());
			candidatePairs = candidatePairs.subList(0, indexRemove);
		}

		// combine the first pair
		// TODO: maybe combine several pairs at once
		if (p != null) {
			System.out.println("time: " + (System.currentTimeMillis() - start));
			partitionSet.remove(p.first());
			partitionSet.remove(p.second());
			lastCombined = p.combine();
			partitionSet.add(lastCombined);

			// mark for removal all pairs that include one of the combined
			// partitions from the candidate pairs
			System.out.println("time: " + (System.currentTimeMillis() - start));
			for (PartitionPair pair : candidatePairs) {
				if (pair.first().equals(p.first())
						|| pair.first().equals(p.second())
						|| pair.second().equals(p.first())
						|| pair.second().equals(p.second())) {
					pair.reductionC = -1;
				}
			}
			System.out.println("time: " + (System.currentTimeMillis() - start)
					+ ", combining " + p.first().r().toString() + ","
					+ p.second().r().toString());
		}
	}

	/**
	 * The getSplit method implementation. (This method is invoked from the
	 * driver class)
	 */
	public List<InputSplit> getSplits() {

		// step 1: get the set of range units
		List<Range> rangelets = getRangelets(joinInput1.getFullRange());

		// step 2: create virtual buckets
		List<VBucket> vbuckets = getVirtualBuckets(
				joinInput2.getBucketRanges(), rangelets);

		// step 3: initialize the partition set
		PartitionSet partitionSet = new PartitionSet(vbuckets, joinInput1);

		// step 4: the heuristic based combine step
		long initialC; // = partitionSet.C();
		do {
			initialC = partitionSet.C();
			System.out.println("iterating, cost before: " + initialC);
			iterate(partitionSet);
		} while ((initialC - partitionSet.C() > 0) && // (i) there is reduction
														// in size
				(partitionSet.size() > minSplits) // (iii) number of partitions
													// greater than threshold
		);

		// step 5: return the final partition set as input splits
		System.out.println("final cost: " + partitionSet.C());
		return partitionSet.getInputSplits();

	}

	/*
	 * 
	 * The helper classes follow below.
	 */

	/**
	 * The physical bucket instance. I guess this cane be replaced with one of
	 * the existing classes, e.g. MDIndex.BucketInfo ?
	 * 
	 * @author alekh
	 */
	public static class PBucket {
		private int id;
		private long size;

		public PBucket(MDIndex.BucketInfo info, HPJoinInput input) {
			id = info.getId();
			size = input.getPartitionIdSizeMap().get(id);
		}

		public int id() {
			return id;
		}

		public long size() {
			return size;
		}
	}

	/**
	 * The virtual bucket instance.
	 * 
	 * @author alekh
	 */
	public static class VBucket {
		private PBucket b;
		private Range r;

		public VBucket(PBucket b, Range r) {
			this.b = b;
			this.r = r;
		}

		public PBucket b() {
			return b;
		}

		public Range range() {
			return r;
		}
	}

	/**
	 * The Partition class which holds a set of virtual buckets.
	 * 
	 * @author alekh
	 */
	public static class Partition {
		private Set<VBucket> vbuckets;
		private Set<PBucket> pbuckets;
		private Range range;
		private long sizeA, sizeB;
		private HPJoinInput secondInput;
		private static Map<Range, Long> rangeLookups = new HashMap<Range, Long>();

		private Partition() {
		}

		public Partition(VBucket vbucket, HPJoinInput secondInput) {
			this.secondInput = secondInput;
			vbuckets = Sets.newHashSet(vbucket);
			pbuckets = Sets.newHashSet(vbucket.b());
			range = vbucket.range().clone(); // we use clone because the range
												// of this partition could be
												// later extended
			sizeA = vbucket.b().size();
			sizeB = lookupSizeB(range);
		}

		protected Partition clone() {
			Partition p = new Partition();
			p.vbuckets = Sets.newHashSet(vbuckets);
			p.pbuckets = Sets.newHashSet(pbuckets);
			p.range = range.clone();
			p.sizeA = sizeA;
			p.sizeB = sizeB;
			p.secondInput = secondInput;
			return p;
		}

		private long lookupSizeB(Range r) {
			// index lookup from the second input
			/*
			 * long[] lengths = secondInput.getLengths(
			 * secondInput.getRangeScan(true, r.getLow(), r.getHigh()) ); long
			 * totalLength = 0; for(long l: lengths) totalLength += l; return
			 * totalLength;
			 */
			if (rangeLookups.containsKey(r)) {
				return rangeLookups.get(r);
			} else {
				long result = secondInput.getTotalSize(secondInput
						.getRangeScan(true, r.getLow(), r.getHigh()));
				rangeLookups.put(r, result);
				return result;
			}
		}

		public Set<VBucket> v() {
			return vbuckets;
		}

		public Set<PBucket> b() {
			return pbuckets;
		}

		public Range r() {
			return range;
		}

		public long sA() {
			return sizeA;
		}

		public long sB() {
			return sizeB;
		}

		public long C() {
			return sA() + sB();
		}
	}

	/**
	 * A pair of partitions which are candidate for combining
	 * 
	 * @author alekh
	 */
	public static class PartitionPair {
		private Partition p1, p2;
		private long combinedSizeA, combinedSizeB;
		private long reductionC = 0;

		public PartitionPair(Partition p1, Partition p2) {
			this.p1 = p1;
			this.p2 = p2;
		}

		public Partition first() {
			return p1;
		}

		public Partition second() {
			return p2;
		}

		public boolean checkAdjacent() {
			if (p1.r().intersectionFraction(p2.r()) <= 0) {
				Range tmp = p1.r().clone();
				tmp.union(p2.r());
				if (tmp.getLength() > p1.r().getLength() + p2.r().getLength()) {
					return false;
				}
			}
			return true;
		}

		/**
		 * Check whether combining this partition pair makes sense or not. (or
		 * is allowed or not)
		 * 
		 * @param maxPartitionSize
		 * @param maxHashTableSize
		 * @return - the reduction in size due to combine, if the combined size
		 *         is less than the max threshold and the hash table size is
		 *         less than the max hash table threshold. - minimum long value
		 *         otherwise.
		 * 
		 */
		public boolean checkCombine(double maxPartitionSize,
				double maxHashTableSize) {
			combinedSA();
			combinedSB();
			return (p1 != p2)
					&& (combinedSizeA + combinedSizeB <= maxPartitionSize) && // max
																				// split
																				// size
																				// check
					(combinedSizeB <= maxHashTableSize); // max hash table size
															// check
		}

		public Partition combine() {
			Partition p3 = p1.clone();
			p3.v().addAll(p2.v());
			p3.b().addAll(p2.b());
			p3.r().union(p2.r());
			p3.sizeA = combinedSA();
			p3.sizeB = combinedSB();
			return p3;
		}

		public long combinedSA() {
			if (combinedSizeA == 0) {
				Set<PBucket> combined = Sets.newHashSet(p1.b());
				combined.addAll(p2.b());
				for (PBucket b : combined) {
					combinedSizeA += b.size();
				}
			}
			return combinedSizeA;
		}

		public long combinedSB() {
			if (combinedSizeB == 0) {
				Range tmp = p1.r().clone();
				tmp.union(p2.r());
				combinedSizeB = p1.lookupSizeB(tmp);
			}
			return combinedSizeB;
		}

		public long combinedC() {
			return combinedSA() + combinedSB();
		}

		public long reductionC() {
			if (reductionC == 0 && p1 != null) {
				reductionC = p1.C() + p2.C() - combinedC();
			}
			return reductionC;
		}
	}

	/**
	 * An instance of a set of partitions.
	 * 
	 * @author alekh
	 */
	public class PartitionSet {
		private List<Partition> partitions;

		public PartitionSet(List<VBucket> vbuckets, HPJoinInput secondInput) {
			partitions = Lists.newArrayList();
			for (VBucket vbucket : vbuckets)
				partitions.add(new Partition(vbucket, secondInput));
		}

		public void add(Partition g) {
			partitions.add(g);
		}

		public void remove(Partition g) {
			partitions.remove(g);
		}

		public int size() {
			return partitions.size();
		}

		public long C() {
			long c = 0;
			for (Partition p : partitions)
				c += p.C();
			return c;
		}

		public List<Partition> getPartitions() {
			return partitions;
		}

		// TODO: need to fix this because the buckets are virtual (and hence
		// range predicates, possibly)
		public List<InputSplit> getInputSplits() {
			List<InputSplit> finalSplits = Lists.newArrayList();

			for (Partition partition : partitions) {
				int[] joinInput2Buckets = new int[partition.b().size()];
				int i = 0;
				for (PBucket pbucket : partition.b()) {
					joinInput2Buckets[i] = pbucket.id;
					i++;
				}
				AccessMethod.PartitionSplit[] splits = joinInput1.getRangeScan(
						true, partition.range.getLow(),
						partition.range.getHigh());

				Path[] input1Paths = joinInput1.getPaths(splits);
				Path[] input2Paths = joinInput2.getPaths(joinInput2Buckets);
				long[] input1Lengths = joinInput1.getLengths(splits);
				long[] input2Lengths = joinInput2.getLengths(joinInput2Buckets);

				InputSplit thissplit = formSplit(input1Paths, input2Paths,
						input1Lengths, input2Lengths);
				finalSplits.add(thissplit);
			}
			return finalSplits;
		}
	}
}
