package perf.benchmark;

import core.access.spark.SparkQueryConf;
import core.access.spark.join.HPJoinInput;
import core.access.spark.join.algo.*;
import core.utils.ConfUtils;
import core.utils.HDFSUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by qui on 7/22/15.
 */
public class TestJoinAlgo {

	String hdfsPath1 = "/user/anil/orders";
	String hdfsPath2 = "/user/anil/repl";
	int rid1 = 0;
	int joinAttribute1 = 0;
	int rid2 = 0;
	int joinAttribute2 = 0;
	FileSystem fs;

	long maxSplitSize;
	int fanout;
	int algoType;

	public List<InputSplit> getSplits(Class<? extends JoinAlgo> algoClass)
			throws Exception {
		ConfUtils cfg = new ConfUtils(BenchmarkSettings.conf);
		Configuration conf = new Configuration();

		conf.set("JOIN_INPUT1", hdfsPath1);
		conf.set("JOIN_INPUT2", hdfsPath2);
		conf.set("JOIN_CONDITION", rid1 + "." + joinAttribute1 + "=" + rid2
				+ "." + joinAttribute2);
		conf.set("HADOOP_NAMENODE", cfg.getHADOOP_NAMENODE());

		SparkQueryConf queryConf = new SparkQueryConf(conf);
		queryConf.setHadoopHome(cfg.getHADOOP_HOME());
		queryConf.setZookeeperHosts(cfg.getZOOKEEPER_HOSTS());
		queryConf.setHDFSReplicationFactor(cfg.getHDFS_REPLICATION_FACTOR());

		fs = HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME());

		Class[] argTypes = new Class[] { HPJoinInput.class, HPJoinInput.class };
		HPJoinInput joinInput1 = new HPJoinInput(conf);
		RemoteIterator<LocatedFileStatus> join1FileIterator = fs.listFiles(
				new Path(conf.get(FileInputFormat.INPUT_DIR, hdfsPath1)), true);
		List<FileStatus> join1Files = new ArrayList<FileStatus>();
		while (join1FileIterator.hasNext()) {
			join1Files.add(join1FileIterator.next());
		}
		joinInput1.initialize(join1Files, queryConf);

		HPJoinInput joinInput2 = new HPJoinInput(conf);
		RemoteIterator<LocatedFileStatus> join2FileIterator = fs.listFiles(
				new Path(conf.get(FileInputFormat.INPUT_DIR, hdfsPath2)), true);
		List<FileStatus> join2Files = new ArrayList<FileStatus>();
		while (join2FileIterator.hasNext()) {
			join2Files.add(join2FileIterator.next());
		}
		joinInput2.initialize(join2Files, queryConf);

		HPJoinInput.MAX_SPLIT_SIZE = maxSplitSize;

		JoinAlgo.SPLIT_FANOUT = fanout;
		JoinAlgo algo = algoClass.getConstructor(argTypes).newInstance(
				new Object[] { joinInput1, joinInput2 });
		return algo.getSplits();
	}

	public void analyzeSplits(List<InputSplit> splits) throws Exception {
		System.out.println("Analyzing splits");
		System.out.println("Number of splits: " + splits.size());

		long totalSize = 0;
		for (InputSplit split : splits) {
			totalSize += split.getLength();
		}
		System.out.println("Total input size       " + totalSize);
		System.out.println("-");

		for (InputSplit split : splits) {
			long hashInputSize = 0;
			Path[] files = ((CombineFileSplit) split).getPaths();
			for (Path file : files) {
				if (file.toString().contains(hdfsPath1)) {
					FileStatus info = fs.getFileStatus(file);
					hashInputSize += info.getLen();
				}
			}
			System.out.println("Split: hash input size " + hashInputSize);
			System.out.println("Split: total size      " + split.getLength());
			System.out.println("-");
		}
		// could also look at deviation in split sizes
	}

	public static void main(String[] args) {
		TestJoinAlgo test = new TestJoinAlgo();
		test.maxSplitSize = Integer.parseInt(args[args.length - 1]) * 1000000L;
		test.fanout = Integer.parseInt(args[args.length - 2]);
		test.algoType = Integer.parseInt(args[args.length - 3]);
		try {
			List<InputSplit> splits;
			switch (test.algoType) {
			case 1:
				splits = test.getSplits(HyperJoinOverlappingRanges.class);
				break;
			case 2:
				splits = test.getSplits(HyperJoinReplicatedBuckets.class);
				break;
			case 3:
				splits = test.getSplits(HyperJoinOverlapReplicaTuned.class);
				break;
			case 4:
				splits = test.getSplits(HyperJoinTuned.class);
				break;
			default:
				splits = test.getSplits(IndexNestedLoopJoin.class);
				break;
			}
			test.analyzeSplits(splits);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
