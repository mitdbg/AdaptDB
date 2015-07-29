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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

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

	private List<PartitionRange> combineRanges(List<PartitionRange> ranges, int numClusters) {

		// hack to make splits larger, so that extra splits don't happen
		// expands them to at least 1 / SPLIT_FANOUT of the full range
		// should be fixed
		double minLength = this.joinInput2.getFullRange().getLength() / SPLIT_FANOUT;
		List<PartitionRange> startingRanges = new ArrayList<PartitionRange>();
		for (PartitionRange range : ranges) {
			if (range.getLength() < minLength) {
				range.expand(minLength / range.getLength() - 1);
			}
			startingRanges.add(new PartitionRange(range.getType(), range.getLow(), range.getHigh()));
		}

		if (startingRanges.size() <= numClusters) {
			return startingRanges;
		}

		// k-means++
		// Chebyshev distance is L_\infinity distance. Could create a custom DistanceMeasure with intersection/union
		KMeansPlusPlusClusterer clusterer = new KMeansPlusPlusClusterer(numClusters, -1, new ChebyshevDistance());
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
				case FLOAT:
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

	private List<PartitionRange> cutOversizeRanges(List<PartitionRange> ranges) {
		List<PartitionRange> result = new ArrayList<PartitionRange>();

		for (PartitionRange range : ranges) {

			// get size of range read from smaller table (hash input size)
			AccessMethod.PartitionSplit[] rangePartitions = joinInput1.getRangeScan(true, range.getLow(), range.getHigh());
			long[] dimensionSizes = joinInput1.getLengths(rangePartitions);
			long dimensionSize = 0;
			for (long size : dimensionSizes) {
				dimensionSize += size;
			}

			if (dimensionSize > MAX_HASH_SIZE) {
				System.out.println("cutting "+range+", over max limit");
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
			}

			// What-if cutting. Not up-to-date (should re-use PartitionRanges instead of creating new ones)
/*
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

			if (rangeSize * 2 < dimensionSize) {
				System.out.println("cutting "+range+", replicating better");
				List<Range> splits = range.split(2);
				for (Range s : splits) {
					result.add(new PartitionRange(s.getType(), s.getLow(), s.getHigh()));
				}
			} else {
				//result.add(new PartitionRange(range.getType(), range.getLow(), range.getHigh()));
			}*/
		}
		return result;
	}

	private static void assignBucketsToClosest(List<PartitionRange> oldRanges, List<PartitionRange> newRanges) {
		for (PartitionRange old : oldRanges) {
			// for each old range, find the new range that is the most similar
			PartitionRange closest = newRanges.get(0);
			double closestFraction = closest.jaccardSimilarity(old);
			for (PartitionRange r : newRanges) {
				if (old.jaccardSimilarity(r) > closestFraction) {
					closest = r;
					closestFraction = old.jaccardSimilarity(r);
				}
			}

			// assign all the buckets from the old range to the most similar new range
			/*for (MDIndex.BucketInfo b : old.getBuckets()) {
				closest.addBucket(b);
			}*/

			// attempt at balancing out ranges
			for (MDIndex.BucketInfo b : old.getBuckets()) {
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
		}
	}

	@Override
	public List<InputSplit> getSplits() {
		List<InputSplit> finalSplits = new ArrayList<InputSplit>();

		List<MDIndex.BucketInfo> bucketRanges = joinInput2.getBucketRanges();
		List<PartitionRange> cutRanges = new ArrayList<PartitionRange>();

		// create a PartitionRange for each bucket, to start
		Range fullRange = joinInput2.getFullRange();
		for (MDIndex.BucketInfo r : bucketRanges) {
			PartitionRange range = new PartitionRange(r.getType(), r.getLow(), r.getHigh());
			range.intersect(fullRange);
			r.intersect(fullRange);
			range.addBucket(r);
			cutRanges.add(range);
		}

		while (cutRanges.size() > SPLIT_FANOUT) {
			List<PartitionRange> combined = combineRanges(cutRanges, SPLIT_FANOUT);
			System.out.println("combined: "+combined.toString());
			Collections.sort(combined, new Comparator<PartitionRange>() {
				@Override
				public int compare(PartitionRange o1, PartitionRange o2) {
					return ((Double)o1.getLength()).compareTo(o2.getLength());
				}
			});

			assignBucketsToClosest(cutRanges, combined);

			cutRanges = this.cutOversizeRanges(combined);
			System.out.println("cut: "+cutRanges.toString());
		}

		List<Tuple2<Range, int[]>> rangesToIds = new ArrayList<Tuple2<Range, int[]>>();
		for (PartitionRange r : cutRanges) {
			List<MDIndex.BucketInfo> buckets = r.getBuckets();
			int[] bucketIds = new int[buckets.size()];
			for (int i = 0; i < bucketIds.length; i++) {
				bucketIds[i] = buckets.get(i).getId();
			}
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
