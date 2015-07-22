package core.access.spark.join;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;

import core.access.AccessMethod.PartitionSplit;
import core.access.PartitionRange;
import core.access.Predicate;
import core.access.Predicate.PREDTYPE;
import core.access.iterator.PartitionIterator;
import core.access.spark.SparkInputFormat.SparkFileSplit;
import core.access.spark.SparkQueryConf;
import core.access.spark.join.SparkJoinRecordReader.JoinTuplePair;
import core.index.MDIndex;
import core.index.key.CartilageIndexKeySet;
import core.index.robusttree.RobustTreeHs;
import core.utils.HDFSUtils;
import core.utils.Range;
import core.utils.TypeUtils.TYPE;
import scala.Tuple2;

public class SparkJoinInputFormat extends FileInputFormat<LongWritable, JoinTuplePair> implements Serializable {

	private static final long serialVersionUID = 1L;
	//private static final Log LOG = LogFactory.getLog(FileInputFormat.class);

	private static final long MAX_SPLIT_SIZE = 160000 * 1000000L;
	private static final int SPLIT_FANOUT = 4;
	private static final double OVERLAP_THRESHOLD = 0.75;
	
	SparkQueryConf queryConf;

	public List<InputSplit> getSplits(JobContext job) throws IOException {
		
		// gather query configuration
		queryConf = new SparkQueryConf(job.getConfiguration());		
		boolean assignBuckets = job.getConfiguration().getBoolean("ASSIGN_BUCKETS", true);	//TODO: this should be a query conf!
		
		// join input 1
		HPJoinInput input1 = new HPJoinInput(job.getConfiguration());
		input1.initialize(listStatus(job), queryConf);
		
		// join input 2
		HPJoinInput input2 = new HPJoinInput(job.getConfiguration());
		input2.initialize(listStatus(job), queryConf);
				
		System.out.println("files from first join input: "+input1.getNumPartitions());
		System.out.println("files from both join inputs: "+input2.getNumPartitions());

		
		
		/*
		List<InputSplit> finalSplits = new ArrayList<InputSplit>();

		int numPerSplit = partitionIdFileMap1.keys().size() / 10;
		int num = 0;
		List<Path> splitFiles = Lists.newArrayList();
		List<Long> lengths = Lists.newArrayList();
		List<Integer> ids = Lists.newArrayList();

		for (Integer id : partitionIdFileMap1.keys()) {

			for (FileStatus fs : partitionIdFileMap1.get(id)) {
				splitFiles.add(fs.getPath());
				lengths.add(fs.getLen());
				ids.add(id);
			}
			num++;

			if (num == numPerSplit) {
				for (Integer i : ids) {
					for (FileStatus fs : partitionIdFileMap2.get(i)) {
						splitFiles.add(fs.getPath());
						lengths.add(fs.getLen());
					}
				}

				long splitSize = 0;
				Path[] splitFilesArr = new Path[splitFiles.size()];
				long[] lengthsArr = new long[lengths.size()];
				for (int i = 0; i < splitFilesArr.length; i++) {
					splitFilesArr[i] = splitFiles.get(i);
					lengthsArr[i] = lengths.get(i);
					splitSize += lengthsArr[i];
				}
				System.out.println("Size of split " + id.toString() + ": " + splitSize);

				SparkFileSplit thissplit = new SparkFileSplit(splitFilesArr, lengthsArr, new PartitionIterator());
				finalSplits.add(thissplit);

				num = 0;
				splitFiles = Lists.newArrayList();
				lengths = Lists.newArrayList();
				ids = Lists.newArrayList();
			}
		}

		return finalSplits;
*/


		List<Tuple2<Range, List<Integer>>> bigSplits; // map from ranges on the smaller table, to partitions on the larger table

		
		// added the next 5 lines just for consistency; need to use the input1 and input2 objects throughout		
		Integer joinKey1 = input1.getJoinKey();
		ArrayListMultimap<Integer,FileStatus> partitionIdFileMap1 = input1.getPartitionIdFileMap();		
		RobustTreeHs index2 = input2.getIndex();
		Integer joinKey2 = input2.getJoinKey();
		ArrayListMultimap<Integer,FileStatus> partitionIdFileMap2 = input2.getPartitionIdFileMap();
		
		
		if (assignBuckets) { // assign bucket ids from larger tables to ranges
			List<PartitionRange> startingRanges = getStartingRanges(index2, joinKey2, true);
			Map<Integer, MDIndex.BucketInfo> bucketRanges = index2.getBucketRanges(joinKey2);
			bigSplits = assignBucketsToSplits(startingRanges, startingRanges.get(startingRanges.size()-1), bucketRanges, partitionIdFileMap2);
		} else { // use index on larger table to get bucket ids for each range
			List<PartitionRange> partitions = getStartingRanges(index2, joinKey2, false);

			bigSplits = new ArrayList<Tuple2<Range, List<Integer>>>();
			for (PartitionRange r : partitions) {
				Predicate lookupPred1 = new Predicate(joinKey2, r.getType(), r.getLow(), PREDTYPE.GT);
				Predicate lookupPred2 = new Predicate(joinKey2, r.getType(), r.getHigh(), PREDTYPE.LEQ);

				List<Integer> ids = new ArrayList<Integer>();
				PartitionSplit[] optSplits = input2.getIndexScan(true, lookupPred1, lookupPred2);
				//PartitionSplit[] optSplits = opt.buildAccessPlan(new Query.FilterQuery(new Predicate[]{lookupPred1, lookupPred2}));
				for (PartitionSplit s : optSplits) {
					for (int i : s.getPartitions()) {
						ids.add(i);
					}
				}
				bigSplits.add(new Tuple2<Range, List<Integer>>(r, ids));
			}
		}

		List<InputSplit> finalSplits = new ArrayList<InputSplit>();
		for(Tuple2<Range, List<Integer>> p: bigSplits){
			
			List<Path> splitFiles = Lists.newArrayList();
			List<Long> lengths = Lists.newArrayList();

			// get type of joinKey1 (might be different than joinKey2 if int vs. long)
			// make sure the predicate values are the correct type
			TYPE[] types = input1.getIndex().dimensionTypes;
			Object lowVal = p._1().getLow();
			Object highVal = p._1().getHigh();
			if (types[joinKey1] == TYPE.LONG) {
				lowVal = ((Number)lowVal).longValue();
				highVal = ((Number)highVal).longValue();
			} else if (types[joinKey1] == TYPE.INT) {
				lowVal = ((Number)lowVal).intValue();
				highVal = ((Number)highVal).intValue();
			}

			// lookup the predicate in the smaller table (filter query)
			Predicate lookupPred1 = new Predicate(joinKey1, types[joinKey1], lowVal, PREDTYPE.GT);
			Predicate lookupPred2 = new Predicate(joinKey1, types[joinKey2], highVal, PREDTYPE.LEQ);

			System.out.println("predicate1: "+lookupPred1);
			System.out.println("predicate2: "+lookupPred2);

			// ids from smaller table that match this range of values
			PartitionSplit[] splits = input2.getIndexScan(true, lookupPred1,lookupPred2);
			//PartitionSplit[] splits = am.getPartitionSplits(new FilterQuery(new Predicate[]{lookupPred1,lookupPred2}), true);
			
			// add files from the smaller input first (build input)
			for(PartitionSplit split: splits){
				int[] partitionIds = split.getPartitions();
				for(int i=0;i<partitionIds.length;i++){
					for(FileStatus fs: partitionIdFileMap1.get(partitionIds[i])){
						splitFiles.add(fs.getPath());
						lengths.add(fs.getLen());
					}
					System.out.print(partitionIds[i]+",");
				}
			}
			System.out.println();
			System.out.println("number of files from the smaller input: "+ splitFiles.size());


			// add files from the larger input (probe input)
			for(Integer input1Id: p._2()){
				for(FileStatus fs: partitionIdFileMap2.get(input1Id)){
					splitFiles.add(fs.getPath());
					lengths.add(fs.getLen());
				}
				System.out.print(input1Id+",");
			}
			System.out.println();

			long splitSize = 0;
			Path[] splitFilesArr = new Path[splitFiles.size()];
			long[] lengthsArr = new long[lengths.size()];
			for(int i=0;i<splitFilesArr.length;i++){
				splitFilesArr[i] = splitFiles.get(i);
				lengthsArr[i] = lengths.get(i);
				splitSize += lengthsArr[i];
			}
			System.out.println("Size of split: "+splitSize);
			SparkFileSplit thissplit = new SparkFileSplit(splitFilesArr, lengthsArr, new PartitionIterator());
			finalSplits.add(thissplit);
		}
		
		System.out.println("Total # of splits: " + finalSplits.size());
		System.out.println("done with getting splits");

		return finalSplits;
	}

	protected List<Tuple2<String, List<Integer>>> parsePredicateRanges(String hdfsFilename, String hadoopHome){
		List<Tuple2<String, List<Integer>>> predicatePartitionIds = new ArrayList<Tuple2<String, List<Integer>>>();

		List<String> lines = HDFSUtils.readHDFSLines(hadoopHome, hdfsFilename);
		for(String line: lines){
			String[] tokens = line.split(";");
			List<Integer> partitionIds = Lists.newArrayList();
			for(String idStr: tokens[0].split(","))
				partitionIds.add(Integer.parseInt(idStr));
			predicatePartitionIds.add(new Tuple2<String, List<Integer>>(tokens[1]+","+tokens[2], partitionIds));
		}

		return predicatePartitionIds;
	}

	private static List<PartitionRange> getStartingRanges(RobustTreeHs index, int key, boolean overlap) {
		List<PartitionRange> partitions = new ArrayList<PartitionRange>();

		CartilageIndexKeySet sample = ((RobustTreeHs) index).sample;
		Object[] cutpoints = sample.getCutpoints(key, SPLIT_FANOUT);
		TYPE type = sample.getTypes()[key];

		Range fullRange = new Range(type, cutpoints[0], cutpoints[cutpoints.length-1]);
		fullRange.expand(0.001);
		cutpoints[0] = fullRange.getLow();
		cutpoints[cutpoints.length-1] = fullRange.getHigh();

		int rangeSize = 1;
		int currentStart = 0;
		int maxRangeSize = overlap ? SPLIT_FANOUT : rangeSize;
		while (rangeSize <= maxRangeSize) {
			while (currentStart < (cutpoints.length - rangeSize)) {
				partitions.add(new PartitionRange(type, cutpoints[currentStart], cutpoints[currentStart + rangeSize]));
				currentStart += rangeSize;
			}
			rangeSize *= 2;
			currentStart = 0;
		}
		System.out.println(partitions);
		return partitions;
	}

	private static List<Tuple2<Range, List<Integer>>> assignBucketsToSplits(List<PartitionRange> ranges, Range fullRange, Map<Integer, MDIndex.BucketInfo> bucketRanges, ArrayListMultimap<Integer,FileStatus> partitionFileStatuses) {
		List<MDIndex.BucketInfo> unassignedBuckets = new ArrayList<MDIndex.BucketInfo>();

		// For every bucket, try to assign it to one of the pre-calculated ranges
		for (Map.Entry<Integer, MDIndex.BucketInfo> bucketRange : bucketRanges.entrySet()) {
			if (!partitionFileStatuses.containsKey(bucketRange.getKey())) {
				continue;
			}

			MDIndex.BucketInfo info = bucketRange.getValue();
			boolean inserted = false;
			for (PartitionRange r : ranges) {
				info.intersect(fullRange);
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
		
		for (MDIndex.BucketInfo info : unassignedBuckets) {
			for (PartitionRange r : ranges) {
				if (info.intersectionFraction(r) > 0) {
					r.addBucket(info, false);
				}
			}
		}

		System.out.println("Ranges formed: "+ranges);
		System.out.println("Unassigned buckets: "+unassignedBuckets);

		// Split up any ranges of buckets that are too large
		List<Tuple2<Range, List<Integer>>> splits = new ArrayList<Tuple2<Range, List<Integer>>>();
		for (PartitionRange r : ranges) {
			List<MDIndex.BucketInfo> buckets = r.getBuckets();
			System.out.println(buckets.size() + " buckets in "+r);

			long bucketSize = 0;
			for (MDIndex.BucketInfo bucket : buckets) {
				List<FileStatus> files = partitionFileStatuses.get(bucket.getId());
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
					List<FileStatus> files = partitionFileStatuses.get(bucket.getId());
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

//	@Override
//	public RecordReader<LongWritable, IteratorRecord> createRecordReader(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
//		return new SparkRecordReader(){
//			long relationId;
//
//			@Override
//			public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
//
//				System.out.println("Initializing SparkRecordReader");
//
//				conf = context.getConfiguration();
//				//client = CuratorUtils.createAndStartClient(conf.get(SparkQueryConf.ZOOKEEPER_HOSTS));
//				client = null;
//				sparkSplit = (SparkFileSplit)split;
//
//				iterator = sparkSplit.getIterator();
//				currentFile = 0;
//
//				//FileSystem fs = sparkSplit.getPath(currentFile).getFileSystem(conf);
//				//counter = new BucketCounts(fs, conf.get(SparkQueryConf.COUNTERS_FILE));
//				//locker = new PartitionLock(fs, conf.get(SparkQueryConf.LOCK_DIR));
//
//				hasNext = initializeNext();
//				key = new LongWritable();
//				recordId = 0;
//			}
//
//			protected boolean initializeNext() throws IOException{
//				boolean flag = super.initializeNext();
//				if(flag) {
//					relationId = sparkSplit.getPath(currentFile-1).toString().contains("repl") ? 1 : 0; // CHECK
//					//relationId = sparkSplit.getPath(currentFile-1).toString().contains(joinInput1) ? rid1 : rid2;
//					//System.out.println(sparkSplit.getPath(currentFile-1).toString()+" "+relationId);
//				}
//				return flag;
//			}
//			public LongWritable getCurrentKey() throws IOException, InterruptedException {
//				key.set(relationId);
//				return key;
//			}
//		};
//	}

	public RecordReader<LongWritable, JoinTuplePair> createRecordReader(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
		return new SparkJoinRecordReader();
	}
}
