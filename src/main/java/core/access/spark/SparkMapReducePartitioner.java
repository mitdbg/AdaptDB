package core.access.spark;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import core.access.iterator.IteratorRecord;
import core.index.robusttree.RNode;
import core.index.robusttree.RobustTree;
import core.utils.HDFSUtils;

public class SparkMapReducePartitioner extends SparkUpfrontPartitioner {

	public SparkMapReducePartitioner(String propertiesFile, String hdfsPath) {
		super(propertiesFile, hdfsPath);
	}

	public static class MyMap implements PairFunction<String, Integer, String> {
		private static final long serialVersionUID = 1L;
		protected IteratorRecord record;
		protected transient RNode newIndexTree;
		protected Tuple2<Integer, String> t;

		public MyMap(RNode newIndexTree) {
			this.record = new IteratorRecord();
			this.newIndexTree = newIndexTree;
			this.t = new Tuple2<Integer, String>(0, "");
		}

		@Override
		public Tuple2<Integer, String> call(String p) {
			record.setBytes(p.getBytes());
			int id = newIndexTree.getBucketId(record);
			return new Tuple2<Integer, String>(id, p);
		}

		private void writeObject(java.io.ObjectOutputStream out)
				throws IOException {
			out.defaultWriteObject();
			String tree = newIndexTree.marshall();
			Text.writeString(out, tree);
		}

		private void readObject(java.io.ObjectInputStream in)
				throws IOException, ClassNotFoundException {
			in.defaultReadObject();
			String tree = Text.readString(in);
			newIndexTree = new RNode();
			newIndexTree.unmarshall(tree.getBytes());
		}
	}

	public static class MyOutputFormat extends
			MultipleTextOutputFormat<Integer, Iterable<String>> {
		public static String HDFS_OUTPUT_PATH = "HDFS_OUTPUT_PATH";

		@Override
		protected String generateFileNameForKeyValue(Integer key,
				Iterable<String> value, String name) {
			return key.toString() + "/" + name;
		}

		protected static class MyRecordWriter implements
				RecordWriter<Integer, Iterable<String>> {
			protected DataOutputStream out;

			public MyRecordWriter(DataOutputStream out) {
				this.out = out;
			}

			@Override
			public void write(Integer arg0, Iterable<String> arg1)
					throws IOException {
				for (String v : arg1)
					out.write(v.getBytes());
			}

			@Override
			public void close(Reporter arg0) throws IOException {
				out.close();
			}
		}

		@Override
		protected RecordWriter<Integer, Iterable<String>> getBaseRecordWriter(
				FileSystem fs, JobConf job, String name, Progressable progress)
				throws IOException {
			Path file = FileOutputFormat.getTaskOutputPath(job, name);
			// FileSystem fs = file.getFileSystem(job);
			FSDataOutputStream fileOut = fs.create(file, progress);
			return new MyRecordWriter(fileOut);
		}
	}

	public void createPartitions(String localDataDir) {

		// read the index
		FileSystem fs = HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME());
		byte[] indexBytes = HDFSUtils.readFile(fs, hdfsPath + "/index");
		RobustTree index = new RobustTree();
		index.unmarshall(indexBytes);
		final RNode t = index.getRoot();

		// create the partitions
		// ctx.hadoopConfiguration().set(MyOutputFormat.HDFS_OUTPUT_PATH,
		// "hdfs://localhost:9000" + hdfsPath + "/partitions");
		ctx.textFile(localDataDir)
				.mapToPair(new MyMap(t))
				.groupByKey()
				.saveAsHadoopFile(
						cfg.getHADOOP_NAMENODE() + hdfsPath + "/partitions",
						Integer.class, Iterable.class, MyOutputFormat.class);

	}

	public static void main(String[] args) {

		// long bucketSize = 64;//*1024*1024l;
		// int scaleFactor = 1000;
		// int scaleFactor = 1;
		// int numBuckets = (int)((scaleFactor * 759) / bucketSize + 1);
		// double samplingRate = (1000 * numBuckets) / ((double)(6000000 *
		// scaleFactor));
		double samplingRate = 0.01;
		System.out.println("sampling rate: " + samplingRate);

		/*
		 * String propertiesFile =
		 * "/Users/alekh/Work/Cartilage/MDindex/conf/cartilage.properties";
		 * String dataDir =
		 * "file:///Users/alekh/Work/Cartilage/support/datasets/scale_1/lineitem.tbl"
		 * ; String hdfsDir = "/user/alekh/singletest";
		 */

		String propertiesFile = "/home/mdindex/cartilage.properties";
		String dataDir = "file:///data/mdindex/tpch-dbgen/lineitem1000/";
		String hdfsDir = "/user/anil/one";

		/*
		 * String propertiesFile =
		 * "/Users/qui/Documents/mdindex/conf/cartilage.properties"; String
		 * dataDir = "/Users/qui/Documents/data/scale_1/"; String hdfsDir =
		 * "/user/qui/dodo";
		 */

		SparkMapReducePartitioner p = new SparkMapReducePartitioner(
				propertiesFile, hdfsDir);
		// p.createIndex(dataDir, numBuckets, samplingRate);
		p.createPartitions(dataDir);
	}

}
