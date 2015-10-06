package core.access.spark;

import java.io.IOException;
import java.util.List;

import junit.framework.TestCase;
import perf.benchmark.BenchmarkSettings;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import core.access.Predicate;
import core.access.Predicate.PREDTYPE;
import core.access.iterator.PartitionIterator;
import core.utils.ConfUtils;
import core.utils.TypeUtils.*;

public class TestSparkInputFormat extends TestCase {

	public static class TestSparkFileSplit extends TestCase {

		// SparkFileSplit sparkSplit;

		Path[] files;
		long[] start;
		long[] lengths;
		String[] locations;
		PartitionIterator iterator;

		public void setUp() {
			files = new Path[] { new Path("path0"), new Path("path1") };
			start = new long[] { 0, 0 };
			lengths = new long[] { 100, 100 };
			locations = new String[] { "location0", "location1" };
			iterator = new PartitionIterator();
		}

		// public void testGetIterator(){
		// sparkSplit = new SparkFileSplit(files, start, lengths, locations,
		// iterator);
		// assertEquals(iterator, sparkSplit.getIterator());
		// }
		//
		// public void testWrite(){
		// sparkSplit = new SparkFileSplit(files, start, lengths, locations,
		// iterator);
		// DataOutput out = new DataOutputStream(new ByteArrayOutputStream());
		// try {
		// sparkSplit.write(out);
		// assert(true);
		// } catch (IOException e) {
		// e.printStackTrace();
		// assert(false);
		// }
		// }
		//
		// public void testReadFields(){
		// sparkSplit = new SparkFileSplit(files, start, lengths, locations,
		// iterator);
		// ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
		// DataOutput out = new DataOutputStream(byteOutputStream);
		// try {
		// sparkSplit.write(out);
		// SparkFileSplit sparkSplit2 = new SparkFileSplit();
		// byte[] buf = byteOutputStream.toByteArray();
		// DataInput in = new DataInputStream(new ByteArrayInputStream(buf));
		// sparkSplit2.readFields(in);
		// for(int i=0;i<sparkSplit2.getNumPaths();i++){
		// assertEquals(files[i], sparkSplit2.getPath(i));
		// assertEquals(start[i], sparkSplit2.getOffset(i));
		// assertEquals(lengths[i], sparkSplit2.getLength(i));
		// }
		// assert(true);
		//
		// } catch (IOException e) {
		// e.printStackTrace();
		// assert(false);
		// }
		// }

		// public void testReadFields2(){
		// iterator = new RepartitionIterator(101, 102);
		// sparkSplit = new SparkFileSplit(files, start, lengths, locations,
		// iterator);
		// ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
		// DataOutput out = new DataOutputStream(byteOutputStream);
		// try {
		// sparkSplit.write(out);
		// SparkFileSplit sparkSplit2 = new SparkFileSplit();
		// byte[] buf = byteOutputStream.toByteArray();
		// DataInput in = new DataInputStream(new ByteArrayInputStream(buf));
		// sparkSplit2.readFields(in);
		// for(int i=0;i<sparkSplit2.getNumPaths();i++){
		// assertEquals(files[i], sparkSplit2.getPath(i));
		// assertEquals(start[i], sparkSplit2.getOffset(i));
		// assertEquals(lengths[i], sparkSplit2.getLength(i));
		// assertEquals(iterator.getClass().getName(),
		// sparkSplit2.getIterator().getClass().getName());
		// }
		// assert(true);
		//
		// } catch (IOException e) {
		// e.printStackTrace();
		// assert(false);
		// }
		// }
	}

	SparkInputFormat sparkInputFormat;
	Job job;

	public void setUp() {

		Predicate[] predicates = new Predicate[] { new Predicate(0, TYPE.INT,
				3002147, PREDTYPE.LEQ) };
		String hdfsPath = "hdfs://localhost:9000/user/alekh/dodo";

		Configuration conf = new Configuration();
		ConfUtils cfg = new ConfUtils(BenchmarkSettings.conf);

		SparkQueryConf queryConf = new SparkQueryConf(conf);
		queryConf.setWorkingDir(hdfsPath);
		queryConf.setQuery(predicates);
		queryConf.setHadoopHome(cfg.getHADOOP_HOME());
		queryConf.setZookeeperHosts(cfg.getZOOKEEPER_HOSTS());
		queryConf.setMaxSplitSize(1024 / 64);

		try {
			job = Job.getInstance(conf);
			FileInputFormat.setInputPaths(job, hdfsPath);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void testGetSplits() {
		try {
			sparkInputFormat = new SparkInputFormat();
			List<InputSplit> splits = sparkInputFormat.getSplits(job);
			System.out.println(splits.size());

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void testCreateRecordReader() {
		try {
			sparkInputFormat = new SparkInputFormat();
			List<InputSplit> splits = sparkInputFormat.getSplits(job);
			for (InputSplit split : splits)
				sparkInputFormat.createRecordReader(split, null);

		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
