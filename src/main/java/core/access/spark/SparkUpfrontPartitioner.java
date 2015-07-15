package core.access.spark;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import core.access.HDFSPartition;
import core.index.build.HDFSPartitionWriter;
import core.index.build.InputReader;
import core.index.key.CartilageIndexKey;
import core.index.robusttree.RobustTreeHs;
import core.utils.HDFSUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import core.access.iterator.DistributedPartitioningIterator;
import core.index.robusttree.RNode;
import core.utils.ConfUtils;

public class SparkUpfrontPartitioner {

	ConfUtils cfg;
	String propertiesFile;
	//private SparkQueryConf queryConf;
	JavaSparkContext ctx;
	String hdfsPath;
	
	public SparkUpfrontPartitioner(String propertiesFile, String hdfsPath){
		this.cfg = new ConfUtils(propertiesFile);
		this.hdfsPath = hdfsPath;
		SparkConf sconf = new SparkConf()
								.setMaster(cfg.getSPARK_MASTER())
								.setAppName(this.getClass().getName())
								.setSparkHome(cfg.getSPARK_HOME())
								.setJars(new String[]{cfg.getSPARK_APPLICATION_JAR()})
								.set("spark.hadoop.cloneConf", "false")
								.set("spark.executor.memory", "120g")
								.set("spark.local.dir", "/data/mdindex/tmp");
		ctx = new JavaSparkContext(sconf);
		this.propertiesFile = propertiesFile;
		//queryConf = new SparkQueryConf(ctx.hadoopConfiguration());
	}

	public void sampleBlocks(String localDataDir, final double samplingRate, String hdfsPath) {
		int numBlocks = (int) (1 / samplingRate * 100); // sample 100 chunks from file
		SparkSamplingMapFunction f = new SparkSamplingMapFunction(samplingRate);
		JavaRDD<String> sample = ctx.textFile(localDataDir, numBlocks).mapPartitions(f);
		//UUID id = UUID.randomUUID();
		//throw new RuntimeException("num "+sample.count());
		sample.coalesce(1).saveAsTextFile(cfg.getHADOOP_NAMENODE() + hdfsPath + "/samples/");
	}

	public void buildIndex(String sampleDir, RNode index) {

	}
	
	
	public void createTextFile(String localDataDir, RNode newIndexTree){
		final DistributedPartitioningIterator partitioner = new DistributedPartitioningIterator(cfg.getZOOKEEPER_HOSTS(), newIndexTree, propertiesFile, hdfsPath);
		FlatMapFunction<Iterator<String>, String> mapFunc = new SparkPartitioningMapFunction(partitioner);

		JavaRDD<String> distFile = ctx.textFile(localDataDir).mapPartitions(mapFunc);
		long lines = distFile.count();
		System.out.println("Number of lines = " + lines);
	}

	public static void main(String[] args) {

		//int bucketSize = 64*1024*1024;
		//int scaleFactor = 10000;
		//int scaleFactor = 1;
		//int numBuckets = (scaleFactor * 759863287) / bucketSize + 1;
		//double samplingRate = (1000 * numBuckets) / ((double)(6000000 * scaleFactor));
		//double samplingRate = 0.01;
		//System.out.println("sampling rate: "+samplingRate);

		String propertiesFile = "/home/mdindex/cartilage.properties";
		String dataDir = "file:///data/mdindex/tpch-dbgen/lineitem1000/";
		String hdfsDir = "/user/anil/one";

		/*
		String propertiesFile = "/Users/qui/Documents/mdindex/conf/cartilage.properties";
		String dataDir = "/Users/qui/Documents/data/scale_1/";
		String hdfsDir = "/user/qui/dodo";
		*/



		// 1. sample from files on each machine, write to hdfs
		//SparkUpfrontPartitioner p = new SparkUpfrontPartitioner(propertiesFile, hdfsDir);
		//p.sampleBlocks(dataDir, samplingRate, hdfsDir);

		/*// 2. on one machine, build the index and write to hdfs.
		RobustTreeHs index = new RobustTreeHs(1);
		index.initBuild();
		CartilageIndexKey key = new CartilageIndexKey('|');
		InputReader r = new InputReader(index, key);
		FileSystem fs = HDFSUtils.getFS(HDFSUtils.getFS(ConfUtils.create(propertiesFile, "defaultHDFSPath").getHadoopHome()+"/etc/hadoop/core-site.xml");)
		r.scanHDFSDirectory(fs, hdfsDir + "/sample");
		index.initProbe();

		HDFSPartitionWriter writer = new HDFSPartitionWriter(hdfsDir, propertiesFile);
		byte[] indexBytes = index.marshall();
		writer.writeToPartition("index", indexBytes, 0, indexBytes.length);
		*/

		// 3. every machine reads the index, and writes the partitions.
		ConfUtils conf = new ConfUtils(propertiesFile);
		FileSystem fs = HDFSUtils.getFS(conf.getHADOOP_HOME() + "/etc/hadoop/core-site.xml");
		byte[] indexBytes = HDFSUtils.readFile(fs, hdfsDir + "/index");
		RobustTreeHs index = new RobustTreeHs(1);
		index.unmarshall(indexBytes);

		SparkUpfrontPartitioner p = new SparkUpfrontPartitioner(propertiesFile, hdfsDir+"/partitions");
		p.createTextFile(dataDir, index.getRoot());

		// deal with long first value*/
	}
}
