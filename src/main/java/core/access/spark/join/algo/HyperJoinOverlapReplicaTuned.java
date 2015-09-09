package core.access.spark.join.algo;

import core.access.AccessMethod;
import core.access.PartitionRange;
import core.access.spark.join.HPJoinInput;
import core.index.MDIndex;
import core.utils.Range;
import core.utils.TypeUtils;
import org.apache.commons.math3.ml.clustering.CentroidCluster;
import org.apache.commons.math3.ml.clustering.KMeansPlusPlusClusterer;
import org.apache.commons.math3.ml.distance.ChebyshevDistance;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import scala.Tuple2;

import java.util.*;

public class HyperJoinOverlapReplicaTuned extends JoinAlgo {

	private static final long MAX_HASH_SIZE = 40000 * 1000000L;
	HPJoinInput joinInput1;
	HPJoinInput joinInput2;

	/*
	 * Things to consider:
	 * 
	 * 1. the available memory
	 * 2. number of splits
	 * 3. size of hash table --> 
	 * 4. the total input sizes
	 *  
	 *  
	 * max split size 
	 */

	
	/*
	 * What options do we have:
	 * 
	 *  1. extend existing split [other table will scan more]
	 *  2. create a new split, when we reach the ahs table size limit 
	 *  3. cut the bucket into two --> try 1. and 2. again
	 *  
	 *  
	 *  
	 *  
	 *  Two STep algo:
	 *  
	 *  1. cutting -- cut buckets that span too much into multiple (virtual) ones so that the hash table fits in memory [here, each bucket is potentially a split]
	 *  2. combine -- so that we reduce the total input size, while keeping the hash table size within limit, and stopping as soon as we reach the minimum number of splits 
	 * 
	 */
	public HyperJoinOverlapReplicaTuned(HPJoinInput joinInput1, HPJoinInput joinInput2) {
		this.joinInput1 = joinInput1;
		this.joinInput2 = joinInput2;
	}

	private List<PartitionRange> clusterRanges(List<PartitionRange> ranges, int numClusters) {
		List<PartitionRange> startingRanges = new ArrayList<PartitionRange>();
		for (PartitionRange range : ranges) {
			startingRanges.add(new PartitionRange(range.getType(), range.getLow(), range.getHigh()));
		}

		if (startingRanges.size() <= numClusters) {
			return startingRanges;
		}

		// k-means++
		// Chebyshev distance is L_\infinity distance. Could create a custom DistanceMeasure with intersection/union
		KMeansPlusPlusClusterer clusterer = new KMeansPlusPlusClusterer(numClusters, -1, new IntervalDistance());
		List<CentroidCluster> clusters = clusterer.cluster(startingRanges);

		// get centers of clusters and use them as the combined ranges
		List<PartitionRange> combined = new ArrayList<PartitionRange>();
		TypeUtils.TYPE type = ranges.get(0).getType();
		for (CentroidCluster cluster : clusters) {
			double[] center = cluster.getCenter().getPoint();
			Object low;
			Object high;
			switch (type) {
				case INT:
					low = (int) center[0];
					high = (int) center[1];
					break;
				case LONG:
					low = (long) center[0];
					high = (long) center[1];
					break;
				case DOUBLE:
					low = (float) center[0];
					high = (float) center[1];
					break;
				default:
					throw new RuntimeException("clustering of "+type.toString()+" not supported");
			}
			combined.add(new PartitionRange(type, low, high));
		}
		return combined;
	}

	private boolean combineRanges(List<PartitionRange> ranges, double threshold) {
		PartitionRange combinee = null;
		for (PartitionRange range : ranges) {
			for (PartitionRange other : ranges) {
				if (!range.equals(other) && range.intersectionFraction(other) > threshold) {
					range.union(other);
					for (MDIndex.BucketInfo b : other.getBuckets()) {
						range.addBucket(b);
					}
					combinee = other;
					break;
				}
			}
			if (combinee != null) {
				break;
			}
		}
		if (combinee != null) {
			ranges.remove(combinee);
			return true;
		} else {
			return false;
		}
	}

	private static PartitionRange getClosestRange(PartitionRange range, List<PartitionRange> possibilities) {
		double[] rangeEnds = range.getPoint();
		PartitionRange closest = null;
		double distance = Double.MAX_VALUE;
		for (PartitionRange possibility : possibilities) {
			double[] ends = possibility.getPoint();
			if (ends[1] < rangeEnds[0]) {
				if (rangeEnds[0] - ends[1] < distance) {
					closest = possibility;
					distance = rangeEnds[0] - ends[1];
				}
			} else if (ends[0] > rangeEnds[1]) {
				if (ends[0] - rangeEnds[1] < distance) {
					closest = possibility;
					distance = ends[0] - rangeEnds[1];
				}
			} else {
				closest = possibility; // intersects
				break;
			}
		}
		return closest;
	}

	private List<PartitionRange> getSplitAssignees(PartitionRange rangeToSplit, List<PartitionRange> possibleRanges, int numCutsNecessary) {
		List<PartitionRange> possibleAssignees = new ArrayList<PartitionRange>(possibleRanges);
		possibleAssignees.remove(rangeToSplit); // add it back later if it doesn't get cut

		TypeUtils.TYPE type = rangeToSplit.getType();
		List<PartitionRange> assignees = new ArrayList<PartitionRange>();
		Object assigneeHigh = rangeToSplit.getLow();

		boolean startCovered = false;
		Object lowestOverlapVal = rangeToSplit.getHigh();
		for (PartitionRange possibleAssignee : possibleAssignees) {
			if (TypeUtils.compareTo(possibleAssignee.getLow(), assigneeHigh, type) <= 0 && TypeUtils.compareTo(possibleAssignee.getHigh(), assigneeHigh, type) > 0) {
				startCovered = true;
			} else if (TypeUtils.compareTo(possibleAssignee.getLow(), assigneeHigh, type) >= 0 && TypeUtils.compareTo(possibleAssignee.getLow(), lowestOverlapVal, type) < 0) {
				lowestOverlapVal = possibleAssignee.getLow();
			}
		}
		if (!startCovered) {
			if (lowestOverlapVal.equals(rangeToSplit.getHigh())) {
				// nothing overlaps at all
				// if not necessary to cut, combine with closest range
				// if necessary to cut, split in half
				if (numCutsNecessary > 0) {
					List<Range> splits = rangeToSplit.split(numCutsNecessary);
					for (Range split : splits) {
						PartitionRange splitRange = new PartitionRange(type, split.getLow(), split.getHigh());
						assignees.add(splitRange);
						possibleAssignees.add(splitRange);
					}
				} else {
					assignees.add(getClosestRange(rangeToSplit, possibleAssignees));
				}
				assigneeHigh = rangeToSplit.getHigh();
			} else {
				PartitionRange start = new PartitionRange(type, rangeToSplit.getLow(), lowestOverlapVal);
				List<Range> splits = start.split(Math.max(numCutsNecessary - assignees.size(), 1));
				for (Range split : splits) {
					PartitionRange splitRange = new PartitionRange(type, split.getLow(), split.getHigh());
					//assignees.add(splitRange);
					possibleAssignees.add(splitRange);
				}
				//possibleAssignees.add(start);
			}
		}

		while (TypeUtils.compareTo(assigneeHigh, rangeToSplit.getHigh(), type) < 0) {
			List<PartitionRange> possibilities = new ArrayList<PartitionRange>();
			for (PartitionRange possibleAssignee : possibleAssignees) {
				if (TypeUtils.compareTo(possibleAssignee.getLow(), assigneeHigh, type) <= 0 && TypeUtils.compareTo(possibleAssignee.getHigh(), assigneeHigh, type) > 0) {
					possibilities.add(possibleAssignee);
				}
			}
			if (possibilities.size() == 0) {
				break;
			} else {
				PartitionRange longestPossibility = possibilities.get(0);
				double longestLength = longestPossibility.getLength();
				for (PartitionRange range : possibilities) {
					if (range.getLength() > longestLength) {
						longestPossibility = range;
						longestLength = range.getLength();
					}
				}
				assignees.add(longestPossibility);
				assigneeHigh = longestPossibility.getHigh();
			}
		}

		if (TypeUtils.compareTo(assigneeHigh, rangeToSplit.getHigh(), type) < 0) {

			PartitionRange end = new PartitionRange(type, assigneeHigh, rangeToSplit.getHigh());
			List<Range> splits = end.split(Math.max(numCutsNecessary - assignees.size(), 1));
			for (Range split : splits) {
				PartitionRange splitRange = new PartitionRange(type, split.getLow(), split.getHigh());
				assignees.add(splitRange);
				possibleAssignees.add(splitRange);
			}
			//possibleAssignees.add(end);
			//assignees.add(end);
		}
		return assignees;

	}

	public void splitRange(PartitionRange rangeToSplit, List<PartitionRange> originalRanges, List<PartitionRange> assignees) {
		originalRanges.remove(rangeToSplit);
		TypeUtils.TYPE type = rangeToSplit.getType();
		Object[] cutpoints = new Object[assignees.size()+1];
		cutpoints[0] = rangeToSplit.getLow();
		for (int i = 0; i < assignees.size(); i++) {
			cutpoints[i+1] = assignees.get(i).getHigh();
		}

		System.out.println("all ranges before: "+originalRanges.toString());
		System.out.println("assignees: "+assignees.toString());
		for (PartitionRange range : assignees) {
			if (!originalRanges.contains(range)) {
				originalRanges.add(range);
			}
		}
		System.out.println("all ranges: "+originalRanges.toString());
		System.out.println("cutpoints for "+rangeToSplit+": "+Arrays.toString(cutpoints));
		for (int i = 0; i < assignees.size(); i++) {
			PartitionRange assignee = assignees.get(i);
			Range currentRange = new Range(type, cutpoints[i], cutpoints[i + 1]);
			for (MDIndex.BucketInfo b : rangeToSplit.getBuckets()) {
				if (b.intersectionFraction(currentRange) > 0) {
					MDIndex.BucketInfo split = b.clone();
					split.intersect(currentRange);
					assignee.addBucket(split);
				}
			}
		}

	}

	private List<PartitionRange> cutOversizeRanges(List<PartitionRange> ranges) {
		Map<PartitionRange, Integer> tooBig = new HashMap<PartitionRange, Integer>();
		List<PartitionRange> fine = new ArrayList<PartitionRange>();
		for (PartitionRange range : ranges) {

			// get size of range read from smaller table (hash input size)
			AccessMethod.PartitionSplit[] rangePartitions = joinInput1.getRangeScan(true, range.getLow(), range.getHigh());
			long[] dimensionSizes = joinInput1.getLengths(rangePartitions);
			long dimensionSize = 0;
			for (long size : dimensionSizes) {
				dimensionSize += size;
			}

			if (dimensionSize > MAX_HASH_SIZE) {
				System.out.println("cutting " + range + ", over max limit");
				tooBig.put(range, (int) Math.ceil(((double) dimensionSize) / MAX_HASH_SIZE));
			} else {
				fine.add(range);
			}
		}

		System.out.println("okay ranges: " + fine.toString());
		for (Map.Entry<PartitionRange, Integer> entry : tooBig.entrySet()) {
			List<PartitionRange> assignees = getSplitAssignees(entry.getKey(), fine, entry.getValue());
			splitRange(entry.getKey(), fine, assignees);
		}
		System.out.println("after splitting max limit: " + fine.toString());

		// after this, every bucket should be assigned to something in "fine"
		List<PartitionRange> result = new ArrayList<PartitionRange>(fine);

		for (PartitionRange range : fine) {
			List<MDIndex.BucketInfo> buckets = range.getBuckets();
			int[] bucketIds = new int[buckets.size()];
			for (int i = 0; i < bucketIds.length; i++) {
				bucketIds[i] = buckets.get(i).getId();
			}
			long[] rangeSizes = joinInput2.getLengths(bucketIds);
			long rangeSize = 0;
			for (long size : rangeSizes) {
				rangeSize += size;
			}

			List<PartitionRange> assignees = getSplitAssignees(range, result, 0);
			// get size of range read from smaller table (hash input size)
			AccessMethod.PartitionSplit[] rangePartitions = joinInput1.getRangeScan(true, range.getLow(), range.getHigh());
			long[] dimensionSizes = joinInput1.getLengths(rangePartitions);
			long dimensionSize = 0;
			for (long size : dimensionSizes) {
				dimensionSize += size;
			}

			long additionalSize = 0;
			boolean overMax = false;
			if (assignees.size() == 1) {
				Range total = range.clone();
				total.union(assignees.get(0));
				AccessMethod.PartitionSplit[] totalPartitions = joinInput1.getRangeScan(true, total.getLow(), total.getHigh());
				long[] totalSizes = joinInput1.getLengths(totalPartitions);
				for (long size : totalSizes) {
					additionalSize += size;
				}
				if (additionalSize > MAX_HASH_SIZE) {
					overMax = true;
				}
				additionalSize -= dimensionSize;
			} else {
				for (PartitionRange assignee : assignees) {
					if (!result.contains(assignee)) {
						AccessMethod.PartitionSplit[] partitions = joinInput1.getRangeScan(true, range.getLow(), range.getHigh());
						long[] sizes = joinInput1.getLengths(partitions);
						for (long size : sizes) {
							additionalSize += size;
						}
					}
				}
			}
			if (!overMax && rangeSize * (assignees.size() - 1) + additionalSize < dimensionSize) {
				System.out.println("cutting "+range+", replicating better");
				System.out.println("benefit: "+(dimensionSize - rangeSize * (assignees.size() - 1) + additionalSize));
				splitRange(range, result, assignees);
			}
		}
		return result;
						/*

				// right now, splits into equal sized pieces without considering what the splits will be combined with
				List<Range> splits = range.split((int) Math.ceil(((double)dimensionSize) / MAX_HASH_SIZE));
				for (Range s : splits) {
					PartitionRange splitRange = new PartitionRange(s.getType(), s.getLow(), s.getHigh());
					for (MDIndex.BucketInfo b : range.getBuckets()) {
						if (b.intersectionFraction(splitRange) > 0) {
							MDIndex.BucketInfo cutBucket = b.clone();
							cutBucket.intersect(splitRange);
							splitRange.addBucket(cutBucket);
						}
					}
					result.add(splitRange);
				}
				//continue;
			} else {
				//result.add(new PartitionRange(range.getType(), range.getLow(), range.getHigh()));
				result.add(range);
			}*/

		// What-if cutting. Not up-to-date (should re-use PartitionRanges instead of creating new ones)
	}

	private static void assignBucketsToClosest(List<PartitionRange> oldRanges, List<PartitionRange> newRanges) {
		for (PartitionRange old : oldRanges) {

			for (MDIndex.BucketInfo b : old.getBuckets()) {
				PartitionRange closest = newRanges.get(0);
				double similarity = b.intersectionFraction(closest);
				for (PartitionRange r : newRanges) {
					if (b.intersectionFraction(r) > similarity) {
						closest = r;
						similarity = b.intersectionFraction(r);
					}
				}
				closest.addBucket(b);
			}
		}
	}

	private static void assignBucketsBalanced(List<PartitionRange> oldRanges, List<PartitionRange> newRanges) {
		List<MDIndex.BucketInfo> multipleMatches = new ArrayList<MDIndex.BucketInfo>();
		List<MDIndex.BucketInfo> noMatches = new ArrayList<MDIndex.BucketInfo>();
		for (PartitionRange old : oldRanges) {

			for (MDIndex.BucketInfo b : old.getBuckets()) {
				int numMatches = 0;
				PartitionRange match = null;
				for (PartitionRange r : newRanges) {
					if (b.intersectionFraction(r) > 0.95) {
						numMatches++;
						match = r;
					}
				}
				if (numMatches > 1) {
					multipleMatches.add(b);
				} else if (numMatches == 1) {
					match.addBucket(b);
				} else {
					noMatches.add(b);
				}
			}
		}

		List<PartitionRange> assignees = new ArrayList<PartitionRange>(newRanges);
		while (multipleMatches.size() > 0) {
			int least = Integer.MAX_VALUE;
			int secondLeast = Integer.MAX_VALUE;
			PartitionRange leastRange = null;
			for (PartitionRange r : assignees) {
				if (r.getNumBuckets() <= least) {
					secondLeast = least;
					least = r.getNumBuckets();
					leastRange = r;
				} else if (r.getNumBuckets() < secondLeast) {
					secondLeast = r.getNumBuckets();
				}
			}

			int numAssigned = 0;
			int toAssign = secondLeast - least;
			//System.out.println("Assigning "+toAssign+" to "+leastRange.toString());
			List<MDIndex.BucketInfo> next = new ArrayList<MDIndex.BucketInfo>();
			for (MDIndex.BucketInfo b : multipleMatches) {
				if (b.intersectionFraction(leastRange) > 0.95 && numAssigned <= toAssign) {
					leastRange.addBucket(b);
					numAssigned++;
				} else {
					next.add(b);
				}
			}
			if (numAssigned == 0) {
				//System.out.println("couldn't assign anything to "+leastRange.toString());
				assignees.remove(leastRange);
			}
			multipleMatches = next;
		}

		for (MDIndex.BucketInfo b : noMatches) {
			PartitionRange closest = newRanges.get(0);
			double similarity = b.intersectionFraction(closest);
			for (PartitionRange r : newRanges) {
				if (b.intersectionFraction(r) > similarity) {
					closest = r;
					similarity = b.intersectionFraction(r);
				}
			}
			closest.addBucket(b);
		}
		/*
			// for each old range, find the new range that is the most similar
			PartitionRange closest = newRanges.get(0);
			double closestFraction = closest.jaccardSimilarity(old);
			for (PartitionRange r : newRanges) {
				if (old.jaccardSimilarity(r) > closestFraction) {
					closest = r;
					closestFraction = old.jaccardSimilarity(r);
				}
			}*/

			// assign all the buckets from the old range to the most similar new range
			/*for (MDIndex.BucketInfo b : old.getBuckets()) {
				closest.addBucket(b);
			}*/

			// attempt at balancing out ranges
		/*	for (MDIndex.BucketInfo b : old.getBuckets()) {
				PartitionRange least = closest;
				int leastNum = least.getNumBuckets();
				for (PartitionRange r : newRanges) {
					if (b.intersectionFraction(r) > .95) {
						if (r.containsBucket(b.getId())) {
							least = r;
							break;
						} else if (r.getNumBuckets() < leastNum) {
							least = r;
							leastNum = r.getNumBuckets();
						}
					}
				}
				least.addBucket(b);
			}
		}*/
	}

	@Override
	public List<InputSplit> getSplits() {
		List<InputSplit> finalSplits = new ArrayList<InputSplit>();

		List<MDIndex.BucketInfo> bucketRanges = joinInput2.getBucketRanges();
		List<PartitionRange> cutRanges = new ArrayList<PartitionRange>();

		// create a PartitionRange for each bucket, to start
		Range fullRange = joinInput2.getFullRange();
		for (MDIndex.BucketInfo r : bucketRanges) {
			r.intersect(fullRange);
			PartitionRange range = new PartitionRange(r.getType(), r.getLow(), r.getHigh());
			range.addBucket(r);
			cutRanges.add(range);
		}

		// first time: cluster
		List<PartitionRange> combined = clusterRanges(cutRanges, SPLIT_FANOUT*2);
		System.out.println("combined: " + combined.toString());
		//assignBucketsToClosest(cutRanges, combined);
		assignBucketsBalanced(cutRanges, combined);
		System.out.println("after assignment: " + combined);
		cutRanges = combined;

		// afterwards: just combine iteratively
		double threshold = 0.95;
		while (cutRanges.size() > SPLIT_FANOUT) {
			cutRanges = this.cutOversizeRanges(cutRanges);
			System.out.println("cut: " + cutRanges.toString());

			boolean lowerThreshold = true;
			while (cutRanges.size() > SPLIT_FANOUT) {
				boolean combinationOccurred = this.combineRanges(cutRanges, threshold);
				if (!combinationOccurred) {
					break;
				} else {
					lowerThreshold = false;
				}
			}

			assignBucketsBalanced(cutRanges, cutRanges);
			System.out.println("after assignment: " + cutRanges);
			if (lowerThreshold) {
				threshold -= .05;
			}
		}

		System.out.println("assigning buckets for final time");
		assignBucketsBalanced(cutRanges, cutRanges);
		List<Tuple2<Range, int[]>> rangesToIds = new ArrayList<Tuple2<Range, int[]>>();
		for (PartitionRange r : cutRanges) {
			System.out.println(r.toString());
			List<MDIndex.BucketInfo> buckets = r.getBuckets();
			int[] bucketIds = new int[buckets.size()];
			for (int i = 0; i < bucketIds.length; i++) {
				bucketIds[i] = buckets.get(i).getId();
			}
			System.out.println(Arrays.toString(bucketIds));
			rangesToIds.add(new Tuple2<Range, int[]>(r, bucketIds));
		}
		for(Tuple2<Range, int[]> r : rangesToIds){
			// ids from smaller table that match this range of values
			AccessMethod.PartitionSplit[] splits = joinInput1.getRangeScan(true, r._1().getLow(), r._1().getHigh());

			Path[] input1Paths = joinInput1.getPaths(splits);
			Path[] input2Paths = joinInput2.getPaths(r._2());
			System.out.println("number of files from the smaller input: "+ input1Paths.length);
			System.out.println("number of files from the larger input: "+ input2Paths.length);

			long[] input1Lengths = joinInput1.getLengths(splits);
			long[] input2Lengths = joinInput2.getLengths(r._2());

			InputSplit thissplit = formSplit(input1Paths, input2Paths, input1Lengths, input2Lengths);
			finalSplits.add(thissplit);
		}
		return finalSplits;
	}
}
