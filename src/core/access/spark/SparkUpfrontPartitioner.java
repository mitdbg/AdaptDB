package core.access.spark;

import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import core.access.iterator.DistributedPartitioningIterator;
import core.index.robusttree.RNode;
import core.utils.ConfUtils;

public class SparkUpfrontPartitioner {

	private ConfUtils cfg;
	private String propertiesFile;
	//private SparkQueryConf queryConf;
	private JavaSparkContext ctx;
	private String hdfsPath;
	
	public SparkUpfrontPartitioner(String propertiesFile, String hdfsPath){
		this.cfg = new ConfUtils(propertiesFile);
		this.hdfsPath = hdfsPath;
		SparkConf sconf = new SparkConf()
								.setMaster(cfg.getSPARK_MASTER())
								.setAppName(this.getClass().getName())
								.setSparkHome(cfg.getSPARK_HOME())
								.setJars(new String[]{cfg.getSPARK_JAR()})
								.set("spark.hadoop.cloneConf", "false")
								.set("spark.executor.memory", "4g");

		ctx = new JavaSparkContext(sconf);
		this.propertiesFile = propertiesFile;
		//queryConf = new SparkQueryConf(ctx.hadoopConfiguration());
	}
	
	
	public void createTextFile(String localDataDir, RNode newIndexTree){
		final DistributedPartitioningIterator partitioner = new DistributedPartitioningIterator(cfg.getZOOKEEPER_HOSTS(), newIndexTree, propertiesFile, hdfsPath);
		FlatMapFunction<Iterator<String>, String> mapFunc = new SparkPartitioningMapFunction(partitioner);

		JavaRDD<String> distFile = ctx.textFile(localDataDir).mapPartitions(mapFunc);
		long lines = distFile.count();
		System.out.println("Number of lines = " + lines);
	}
}
