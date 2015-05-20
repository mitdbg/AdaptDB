package core.access.spark;

import java.util.Iterator;

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

	private ConfUtils cfg;
	//private SparkQueryConf queryConf;
	private JavaSparkContext ctx;
	
	public SparkUpfrontPartitioner(ConfUtils config){
		this.cfg = config;
		SparkConf sconf = new SparkConf()
								.setMaster(cfg.getSPARK_MASTER())
								.setAppName(this.getClass().getName())
								.setSparkHome(cfg.getSPARK_HOME())
								.setJars(new String[]{cfg.getSPARK_JAR()})
								.set("spark.hadoop.cloneConf", "false")
								.set("spark.executor.memory", "4g");

		ctx = new JavaSparkContext(sconf);
		//queryConf = new SparkQueryConf(ctx.hadoopConfiguration());
	}
	
	
	public void createTextFile(String localDataDir, RNode newIndexTree){

		FileSystem hdfs = HDFSUtils.getFS(cfg.getHADOOP_HOME() + "/etc/hadoop/core-site.xml");
		final DistributedPartitioningIterator partitioner = new DistributedPartitioningIterator(null, newIndexTree, hdfs);

		
		JavaRDD<String> distFile = ctx.textFile(localDataDir).mapPartitions(
				
				new FlatMapFunction<Iterator<String>, String>(){
					
					private static final long serialVersionUID = 1L;
					
					public Iterable<String> call(Iterator<String> arg0) throws Exception {
						partitioner.setIterator(arg0);
						while(partitioner.hasNext());
						partitioner.finish();
						return null;	//TODO: check
					}
					
				}
			);
		long lines = distFile.count();
		
		
		
		System.out.println("Number of lines = "+lines);
	}
}
