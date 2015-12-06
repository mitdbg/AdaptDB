package core.join;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.google.common.collect.Lists;

import core.adapt.spark.SparkQueryConf;
import core.join.SparkJoinRecordReader.JoinTuplePair;
import core.join.algo.HyperJoinOverlappingRanges;
import core.join.algo.IndexNestedLoopJoin;
import core.join.algo.JoinAlgo;
import core.utils.HDFSUtils;
import scala.Tuple2;

public class SparkJoinInputFormat extends
		FileInputFormat<LongWritable, JoinTuplePair> implements Serializable {

	private static final long serialVersionUID = 1L;
	// private static final Log LOG = LogFactory.getLog(FileInputFormat.class);

	SparkQueryConf queryConf;

	public List<InputSplit> getSplits(JobContext job) throws IOException {

		// gather query configuration
		queryConf = new SparkQueryConf(job.getConfiguration());
		boolean assignBuckets = job.getConfiguration().getBoolean(
				"ASSIGN_BUCKETS", true); // TODO: this should be a query conf!

		// join input 1
		HPJoinInput input1 = new HPJoinInput(job.getConfiguration());
		input1.initialize(listStatus(job), queryConf);

		// join input 2
		HPJoinInput input2 = new HPJoinInput(job.getConfiguration());
		input2.initialize(listStatus(job), queryConf);

		System.out.println("files from first join input: "
				+ input1.getNumPartitions());
		System.out.println("files from both join inputs: "
				+ input2.getNumPartitions());

		JoinAlgo joinAlgo;
		assignBuckets = false;
		System.out.println("Value of assignBuckets:" + assignBuckets);
		if (assignBuckets) { // assign bucket ids from larger tables to ranges
			joinAlgo = new HyperJoinOverlappingRanges(input1, input2);
		} else {
			joinAlgo = new IndexNestedLoopJoin(input1, input2);
		}
		List<InputSplit> finalSplits = joinAlgo.getSplits();
		System.out.println("Total # of splits: " + finalSplits.size());
		System.out.println("done with getting splits");

		return finalSplits;
	}

	protected List<Tuple2<String, List<Integer>>> parsePredicateRanges(
			String hdfsFilename, String hadoopHome) {
		List<Tuple2<String, List<Integer>>> predicatePartitionIds = new ArrayList<Tuple2<String, List<Integer>>>();

		List<String> lines = HDFSUtils.readHDFSLines(hadoopHome, hdfsFilename);
		for (String line : lines) {
			String[] tokens = line.split(";");
			List<Integer> partitionIds = Lists.newArrayList();
			for (String idStr : tokens[0].split(","))
				partitionIds.add(Integer.parseInt(idStr));
			predicatePartitionIds.add(new Tuple2<String, List<Integer>>(
					tokens[1] + "," + tokens[2], partitionIds));
		}

		return predicatePartitionIds;
	}

	// @Override
	// public RecordReader<LongWritable, IteratorRecord>
	// createRecordReader(InputSplit arg0, TaskAttemptContext arg1) throws
	// IOException, InterruptedException {
	// return new SparkRecordReader(){
	// long relationId;
	//
	// @Override
	// public void initialize(InputSplit split, TaskAttemptContext context)
	// throws IOException, InterruptedException {
	//
	// System.out.println("Initializing SparkRecordReader");
	//
	// conf = context.getConfiguration();
	// //client =
	// CuratorUtils.createAndStartClient(conf.get(SparkQueryConf.ZOOKEEPER_HOSTS));
	// client = null;
	// sparkSplit = (SparkFileSplit)split;
	//
	// iterator = sparkSplit.getIterator();
	// currentFile = 0;
	//
	// //FileSystem fs = sparkSplit.getPath(currentFile).getFileSystem(conf);
	// //counter = new BucketCounts(fs, conf.get(SparkQueryConf.COUNTERS_FILE));
	// //locker = new PartitionLock(fs, conf.get(SparkQueryConf.LOCK_DIR));
	//
	// hasNext = initializeNext();
	// key = new LongWritable();
	// recordId = 0;
	// }
	//
	// protected boolean initializeNext() throws IOException{
	// boolean flag = super.initializeNext();
	// if(flag) {
	// relationId =
	// sparkSplit.getPath(currentFile-1).toString().contains("repl") ? 1 : 0; //
	// CHECK
	// //relationId =
	// sparkSplit.getPath(currentFile-1).toString().contains(joinInput1) ? rid1
	// : rid2;
	// //System.out.println(sparkSplit.getPath(currentFile-1).toString()+" "+relationId);
	// }
	// return flag;
	// }
	// public LongWritable getCurrentKey() throws IOException,
	// InterruptedException {
	// key.set(relationId);
	// return key;
	// }
	// };
	// }

	public RecordReader<LongWritable, JoinTuplePair> createRecordReader(
			InputSplit arg0, TaskAttemptContext arg1) throws IOException,
			InterruptedException {
		return new SparkJoinRecordReader();
	}
}
