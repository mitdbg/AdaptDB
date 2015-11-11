package core.access.spark.join;

import core.adapt.spark.SparkQueryConf;
import core.join.SparkJoinInputFormat;
import core.utils.ConfUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import perf.benchmark.BenchmarkSettings;

import java.io.IOException;
import java.util.List;

/**
 * Created by qui on 7/22/15.
 */
public class TestSparkJoinInputFormat {

	SparkJoinInputFormat inputFormat;
	Job job;
	String hdfsPath1 = "/user/qui/copy";
	String hdfsPath2 = "/user/qui/repl";
	int rid1 = 0;
	int joinAttribute1 = 0;
	int rid2 = 1;
	int joinAttribute2 = 1;

	public void setUp() {
		Configuration conf = new Configuration();
		ConfUtils cfg = new ConfUtils(BenchmarkSettings.conf);

		conf.setBoolean("ASSIGN_BUCKETS", true);
		conf.set("JOIN_INPUT1", hdfsPath1);
		conf.set("JOIN_INPUT2", hdfsPath2);
		conf.set("JOIN_CONDITION", rid1 + "." + joinAttribute1 + "=" + rid2
				+ "." + joinAttribute2);
		conf.set("HADOOP_NAMENODE", cfg.getHADOOP_NAMENODE());

		SparkQueryConf queryConf = new SparkQueryConf(conf);
		queryConf.setHadoopHome(cfg.getHADOOP_HOME());
		queryConf.setZookeeperHosts(cfg.getZOOKEEPER_HOSTS());
		queryConf.setHDFSReplicationFactor(cfg.getHDFS_REPLICATION_FACTOR());

		try {
			job = Job.getInstance(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void testGetSplits() {
		try {
			inputFormat = new SparkJoinInputFormat();
			List<InputSplit> splits = inputFormat.getSplits(job);
			long totalSize = 0;
			for (InputSplit s : splits) {
				totalSize += s.getLength();
			}
			System.out.println("Total size read: " + totalSize);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		TestSparkJoinInputFormat test = new TestSparkJoinInputFormat();
		test.setUp();
		test.testGetSplits();
	}
}
