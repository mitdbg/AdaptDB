package core.index.build.spark;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import core.access.iterator.IteratorRecord;
import core.index.build.HDFSPartitionWriter;
import core.index.build.InputReader;
import core.index.key.CartilageIndexKey;
import core.index.robusttree.RNode;
import core.index.robusttree.RobustTreeHs;
import core.utils.HDFSUtils;

public class SparkMapReducePartitioner extends SparkUpfrontPartitioner {
	
	public SparkMapReducePartitioner(String propertiesFile, String hdfsPath){
		super(propertiesFile, hdfsPath);
	}
	
	
	public static class MyMap implements PairFunction<String, Integer, String> {
		private static final long serialVersionUID = 1L;
		protected IteratorRecord record;
		protected RNode newIndexTree;
		protected Tuple2<Integer, String> t;
		public MyMap(RNode newIndexTree) {
			this.record = new IteratorRecord();
			this.newIndexTree = newIndexTree;
			this.t = new Tuple2<Integer, String>(0,"");
		}
		public Tuple2<Integer, String> call(String p) {
			record.setBytes(p.getBytes());
			int id = newIndexTree.getBucketId(record);
			return new Tuple2<Integer, String>(id,p);
		}
	}
	
	public static class MyOutputFormat extends MultipleTextOutputFormat<Integer, String> {
		
		public static String HDFS_OUTPUT_PATH = "HDFS_OUTPUT_PATH"; 
		
		public String getPartitionPath(JobConf myJob, Integer partitionId){
			return myJob.get(HDFS_OUTPUT_PATH)+"/"+partitionId;
		}
		
	    public RecordWriter<Integer,String> getRecordWriter(FileSystem fs, JobConf job,
	    		String name, Progressable arg3) throws IOException {

	    	final JobConf myJob = job;
	    	
	    	return new RecordWriter<Integer,String>() {
	    		
	    		TreeMap<Integer, DataOutputStream> recordWriters = new TreeMap<Integer, DataOutputStream>();
	    		
	    		public void write(Integer key, String value) throws IOException {	    			
	    			DataOutputStream rw = this.recordWriters.get(key);
	    			if (rw == null) {
	    				Path file = new Path(getPartitionPath(myJob,key));
	    				FileSystem fs = file.getFileSystem(myJob);
	    				FSDataOutputStream fileOut = fs.create(file, false);
	    				this.recordWriters.put(key, fileOut);
	    			}
	    			rw.write(value.getBytes());
	    		};
	    		
	    		public void close(Reporter reporter) throws IOException {
	    			Iterator<Integer> keys = this.recordWriters.keySet().iterator();
	    			while (keys.hasNext())
	    				this.recordWriters.get(keys.next()).close();
	    			this.recordWriters.clear();
	    		};
	    	};
	    }
	}
	
	public void createPartitions(String localDataDir){
		
		// read the index
		FileSystem fs = HDFSUtils.getFS(cfg.getHADOOP_HOME());
		byte[] indexBytes = HDFSUtils.readFile(fs, hdfsPath + "/index");
		RobustTreeHs index = new RobustTreeHs(1);
		index.unmarshall(indexBytes);
		final RNode t = index.getRoot();
		
		// create the partitions
		ctx.hadoopConfiguration().set(MyOutputFormat.HDFS_OUTPUT_PATH, hdfsPath);
		ctx.textFile(localDataDir)
			.mapToPair(new MyMap(t))
			.groupByKey()
			.saveAsHadoopFile("random path", String.class, String.class, MyOutputFormat.class);
		
	}
	
	
	public void createIndex(String localDataDir, int numBuckets, double samplingRate){
		
		// collect samples
		sampleBlocks(localDataDir, samplingRate, hdfsPath);
		
		// build the index tree
		RobustTreeHs index = new RobustTreeHs(1);
		index.initBuild(numBuckets);
		CartilageIndexKey key = new CartilageIndexKey('|');
		InputReader r = new InputReader(index, key);
		FileSystem fs = HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME());
		r.scanHDFSDirectory(fs, hdfsPath + "/samples");
		index.initProbe();

		// write the index to hdfs
		HDFSPartitionWriter writer = new HDFSPartitionWriter(hdfsPath, propertiesFile);
		byte[] indexBytes = index.marshall();
		writer.writeToPartition("index", indexBytes, 0, indexBytes.length);
	}
	
	
	public static void main(String[] args) {

		long bucketSize = 64*1024*1024l;
		//int scaleFactor = 10000;
		int scaleFactor = 1;
		int numBuckets = (int)((scaleFactor * 759863287l) / bucketSize + 1);
		//double samplingRate = (1000 * numBuckets) / ((double)(6000000 * scaleFactor));
		double samplingRate = 0.01;
		System.out.println("sampling rate: "+samplingRate);

		String propertiesFile = "/Users/alekh/Work/Cartilage/MDindex/conf/cartilage.properties";
		String dataDir = "file:///Users/alekh/Work/Cartilage/support/datasets/scale_1/lineitem.tbl";
		String hdfsDir = "/user/alekh/singletest";

		
		SparkMapReducePartitioner p = new SparkMapReducePartitioner(propertiesFile, hdfsDir);
		p.createIndex(dataDir, numBuckets, samplingRate);
		p.createPartitions(dataDir);
	}
	
}
