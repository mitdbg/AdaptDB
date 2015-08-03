package core.access.spark.join;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import core.access.PartitionRange;
import core.access.Predicate;
import core.index.MDIndex;
import core.index.key.CartilageIndexKeySet;
import core.utils.Range;
import core.utils.TypeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import core.access.AccessMethod;
import core.access.AccessMethod.PartitionSplit;
import core.access.spark.HPInput;
import core.access.spark.SparkQueryConf;
import scala.Tuple2;

public class HPJoinInput extends HPInput{

	private static double OVERLAP_THRESHOLD = 0.95;
	public static long MAX_SPLIT_SIZE = 160000 * 1000000L;

	private String joinInput;
	private Integer joinKey;
	@SuppressWarnings("unused")
	private Integer rid;
	
	// the index number of this input amongst the join relations
	private static int idx = 0;
	
	// hard coded .. must be configured
	int joinReplica = 0;
	
	
	public HPJoinInput(Configuration conf){
		joinInput = conf.get("JOIN_INPUT"+(idx+1));
		String joinCond = conf.get("JOIN_CONDITION");
		String hadoopNamenode = conf.get("HADOOP_NAMENODE");
		String tokens[] = joinCond.split("=");
		rid = Integer.parseInt(tokens[idx].split("\\.")[0]);
		joinKey = Integer.parseInt(tokens[idx].split("\\.")[1]);
		idx++;	// increment the idx for the next instantiation of HPJoinInput
		
		conf.set(FileInputFormat.INPUT_DIR, hadoopNamenode + joinInput + "/" + joinReplica);
	}
	
	public void initialize(List<FileStatus> files, SparkQueryConf queryConf){
		AccessMethod am = new AccessMethod();
		queryConf.setWorkingDir(joinInput);
		queryConf.setReplicaId(joinReplica);
		am.init(queryConf);
		super.initialize(files, am);
	}

	public List<MDIndex.BucketInfo> getBucketRanges() {
		return new ArrayList<MDIndex.BucketInfo>(am.getIndex().getBucketRanges(joinKey).values());
	}

	public List<Range> getRangeSplits(int fanout, boolean overlap) {
		List<Range> partitions = new ArrayList<Range>();

		CartilageIndexKeySet sample = am.getIndex().sample;
		Object[] cutpoints = sample.getCutpoints(joinKey, fanout);
		TypeUtils.TYPE type = sample.getTypes()[joinKey];

		Range fullRange = new Range(type, cutpoints[0], cutpoints[cutpoints.length-1]);
		fullRange.expand(0.001);
		cutpoints[0] = fullRange.getLow();
		cutpoints[cutpoints.length-1] = fullRange.getHigh();

		int rangeSize = 1;
		int currentStart = 0;
		int maxRangeSize = overlap ? fanout : rangeSize;
		while (rangeSize <= maxRangeSize) {
			while (currentStart < (cutpoints.length - rangeSize)) {
				partitions.add(new PartitionRange(type, cutpoints[currentStart], cutpoints[currentStart + rangeSize]));
				currentStart += rangeSize;
			}
			rangeSize *= 2;
			currentStart = 0;
		}
		return partitions;
	}

	public List<Tuple2<Range, int[]>> getAssignedBucketSplits(int fanout, boolean overlap) {
		List<Range> ranges = getRangeSplits(fanout, overlap);
		List<PartitionRange> partitions = new ArrayList<PartitionRange>();
		for (Range r : ranges) {
			partitions.add(new PartitionRange(r.getType(), r.getLow(), r.getHigh()));
		}
		List<Tuple2<Range, List<Integer>>> bucketLists = assignBucketsToRanges(partitions);
		List<Tuple2<Range, int[]>> result = new ArrayList<Tuple2<Range, int[]>>();
		for (Tuple2<Range, List<Integer>> list : bucketLists) {
			int[] ids = new int[list._2().size()];
			for (int i = 0; i < ids.length; i++) {
				ids[i] = list._2().get(i);
			}
			result.add(new Tuple2<Range, int[]>(list._1(), ids));
		}
		return result;
	}

	private List<Tuple2<Range, List<Integer>>> assignBucketsToRanges(List<PartitionRange> ranges) {
		List<MDIndex.BucketInfo> unassignedBuckets = new ArrayList<MDIndex.BucketInfo>();
		Range fullRange = getFullRange();
		Map<Integer, MDIndex.BucketInfo> bucketRanges = am.getIndex().getBucketRanges(joinKey);

		// For every bucket, try to assign it to one of the pre-calculated ranges
		for (Map.Entry<Integer, MDIndex.BucketInfo> bucketRange : bucketRanges.entrySet()) {
			if (!partitionIdFileMap.containsKey(bucketRange.getKey())) {
				continue;
			}

			MDIndex.BucketInfo info = bucketRange.getValue();
			boolean inserted = false;
			info.intersect(fullRange);
			for (PartitionRange r : ranges) {
				if (r.contains(info)) {
					r.addBucket(info);
					inserted = true;
					break;
				} else {
					if (info.intersectionFraction(r) > OVERLAP_THRESHOLD) {
						r.addBucket(info);
						inserted = true;
						break;
					}
				}
			}
			if (!inserted) {
				unassignedBuckets.add(info);
			}
		}

		List<MDIndex.BucketInfo> toAssign = unassignedBuckets;
		while (toAssign.size() > 0) {
			List<MDIndex.BucketInfo> current = toAssign;
			toAssign = new ArrayList<MDIndex.BucketInfo>();
			for (MDIndex.BucketInfo info : current) {
				//Collections.shuffle(ranges);
				for (PartitionRange r : ranges) {
					if (info.intersectionFraction(r) == 1.0) {
						r.addBucket(info, false);
						break;
					} else if (info.intersectionFraction(r) > 0) {
						MDIndex.BucketInfo left = info.clone();
						left.subtractLeft(r);
						r.addBucket(info, false);
						if (left.getLength() > 0) {
							toAssign.add(left);
						}
						break;
					}
				}
			}
		}

		System.out.println("Ranges formed: "+ranges);
		System.out.println("Unassigned buckets: " + unassignedBuckets.size());

		// Split up any ranges of buckets that are too large
		List<Tuple2<Range, List<Integer>>> splits = new ArrayList<Tuple2<Range, List<Integer>>>();
		for (PartitionRange r : ranges) {
			List<MDIndex.BucketInfo> buckets = r.getBuckets();
			System.out.println(buckets.size() + " buckets in "+r);

			long bucketSize = 0;
			for (MDIndex.BucketInfo bucket : buckets) {
				List<FileStatus> files = partitionIdFileMap.get(bucket.getId());
				for (FileStatus f : files) {
					bucketSize += f.getLen();
				}
			}
			int numSplitsRequired = (int) (bucketSize / MAX_SPLIT_SIZE + 1);
			long goalSize = bucketSize / numSplitsRequired + 1;

			int currentIndex = 0;
			List<Integer> currentBuckets = new ArrayList<Integer>();
			long totalSize = 0;
			while (currentIndex < buckets.size()) {
				while (totalSize < goalSize && currentIndex < buckets.size()) {
					MDIndex.BucketInfo bucket = buckets.get(currentIndex);
					List<FileStatus> files = partitionIdFileMap.get(bucket.getId());
					currentBuckets.add(bucket.getId());
					for (FileStatus f : files) {
						totalSize += f.getLen();
					}
					currentIndex++;
				}
				splits.add(new Tuple2<Range, List<Integer>>(r, currentBuckets));

				currentBuckets = new ArrayList<Integer>();
				totalSize = 0;
			}
		}
		return splits;
	}
	
	public List<int[]> getEvenBucketSplits(int numSplits) {
		List<int[]> splits = new ArrayList<int[]>();
		double numPerSplit = Math.max(partitionIdFileMap.keys().size() / (double) numSplits, 1);
		int num = 0;
		double target = numPerSplit;
		int[] ids = new int[(int)numPerSplit];

		for (Integer id : partitionIdFileMap.keys()) {
			int intTarget = (int) target;
			ids[intTarget-num-1] = id;
			num++;

			if (num >= intTarget) {
				splits.add(ids);
				System.out.println("Num buckets in split: "+ids.length);
				double newTarget = target + numPerSplit;
				ids = new int[(int)(newTarget - intTarget)];
				target = newTarget;
			}
		}
		return splits;
	}

	public PartitionSplit[] getRangeScan(boolean justAccess, Object lowVal, Object highVal) {
		TypeUtils.TYPE[] types = am.getIndex().dimensionTypes;
		if (types[joinKey] == TypeUtils.TYPE.LONG) {
			lowVal = ((Number)lowVal).longValue();
			highVal = ((Number)highVal).longValue();
		} else if (types[joinKey] == TypeUtils.TYPE.INT) {
			lowVal = ((Number)lowVal).intValue();
			highVal = ((Number)highVal).intValue();
		}

		// lookup the predicate in the smaller table (filter query)
		Predicate lookupPred1 = new Predicate(joinKey, types[joinKey], lowVal, Predicate.PREDTYPE.GT);
		Predicate lookupPred2 = new Predicate(joinKey, types[joinKey], highVal, Predicate.PREDTYPE.LEQ);

		return getIndexScan(justAccess, lookupPred1, lookupPred2);
	}
	
	// utility methods

	public Range getFullRange() {
		CartilageIndexKeySet sample = am.getIndex().sample;
		Object[] cutpoints = sample.getCutpoints(joinKey, 1);
		TypeUtils.TYPE type = sample.getTypes()[joinKey];

		Range fullRange = new Range(type, cutpoints[0], cutpoints[1]);
		fullRange.expand(0.001);
		return fullRange;
	}
}
