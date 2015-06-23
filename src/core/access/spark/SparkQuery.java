package core.access.spark;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import core.access.Predicate;
import core.access.iterator.IteratorRecord;
import core.utils.ConfUtils;

public class SparkQuery {
	private SparkQueryConf queryConf;
	private JavaSparkContext ctx;
	private ConfUtils cfg;

	public SparkQuery(ConfUtils config) {
		this.cfg = config;
		//ctx = new JavaSparkContext(cfg.getSPARK_MASTER(), this.getClass().getName());


		SparkConf sconf = new SparkConf()
								.setMaster(cfg.getSPARK_MASTER())
								.setAppName(this.getClass().getName())
								.setSparkHome(cfg.getSPARK_HOME())
								.setJars(new String[]{cfg.getSPARK_JAR()})
								.set("spark.hadoop.cloneConf", "false")
								.set("spark.executor.memory", "100g")
								.set("spark.driver.memory", "10g")
								.set("spark.task.cpus", "8");
				

		ctx = new JavaSparkContext(sconf);
		//ctx = new JavaSparkContext(cfg.getSPARK_MASTER(), this.getClass().getName(), cfg.getSPARK_HOME(), cfg.getSPARK_JAR());
		ctx.hadoopConfiguration().setBoolean(FileInputFormat.INPUT_DIR_RECURSIVE, true);
		ctx.hadoopConfiguration().set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		queryConf = new SparkQueryConf(ctx.hadoopConfiguration());
	}

//	public void createTextFile(String localPath, Predicate... ps){
//		JavaRDD<String> distFile = ctx.textFile(localPath);
//		long lines = distFile.count();
//		System.out.println("Number of lines = "+lines);
//	}
//
//	public void createHadoopFile(String hdfsPath, Predicate... ps){
//		JavaPairRDD<LongWritable,Text> distFile = ctx.hadoopFile(hdfsPath, TextInputFormat.class, LongWritable.class, Text.class);
//		long lines = distFile.count();
//		System.out.println("Number of lines = "+lines);
//	}
//
//	public void createHadoopRDD(String hdfsPath, Predicate... ps){
//		JobConf conf = new JobConf(ctx.hadoopConfiguration());
//		FileInputFormat.setInputPaths(conf, hdfsPath);
//
//		JavaPairRDD<LongWritable,Text> distFile = ctx.hadoopRDD(conf, TextInputFormat.class, LongWritable.class, Text.class);
//		long lines = distFile.count();
//		System.out.println("Number of lines = "+lines);
//	}
//	
//	public void createNewAPIHadoopRDD(String hdfsPath, Predicate... ps){
//
//		queryConf.setDataset(hdfsPath);
//		queryConf.setPredicates(predicates);
//		queryConf.setWorkers(cfg.getNUM_RACKS() * cfg.getNODES_PER_RACK() * cfg.getMAP_TASKS());
//		queryConf.setHadoopHome(cfg.getHADOOP_HOME());
//		queryConf.setZookeeperHosts(cfg.getZOOKEEPER_HOSTS());
//		queryConf.setMaxSplitSize(4096 / 128);	// number of 64 MB partitions that can fit for each worker (we assume 1GB memory for each worker)
//
//		JavaPairRDD<LongWritable,Text> distFile = ctx.newAPIHadoopFile(
//				cfg.getHADOOP_NAMENODE() +  hdfsPath,
//				org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class,
//				LongWritable.class,
//				Text.class,
//				ctx.hadoopConfiguration()
//			);
//
//		//JavaPairRDD<LongWritable,Text> distFile = ctx.hadoopRDD(conf, TextInputFormat.class, LongWritable.class, Text.class);
//		long lines = distFile.count();
//		System.out.println("Number of lines = "+lines);
//	}

	public JavaPairRDD<LongWritable,IteratorRecord> createRDD(String hdfsPath, Predicate... ps){
		queryConf.setDataset(hdfsPath);
		queryConf.setPredicates(ps);
		queryConf.setWorkers(cfg.getNUM_RACKS() * cfg.getNODES_PER_RACK() * cfg.getMAP_TASKS());
		queryConf.setHadoopHome(cfg.getHADOOP_HOME());
		queryConf.setZookeeperHosts(cfg.getZOOKEEPER_HOSTS());
		queryConf.setMaxSplitSize(8589934592l);	// 8gb is the max size for each split (with 8 threads in parallel)
		queryConf.setMinSplitSize(4294967296l);	// 4gb
		queryConf.setCountersFile(cfg.get("COUNTERS_FILE"));
		queryConf.setCountersFile(cfg.get("LOCK_DIR"));
		
		// ctx.hadoopConfiguration().setClass(FileInputFormat.PATHFILTER_CLASS, SparkPathFilter.class, PathFilter.class);

//		System.setProperty("spark.executor.memory","4g");
//		System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

		return ctx.newAPIHadoopFile(
				cfg.getHADOOP_NAMENODE() +  hdfsPath,
				SparkInputFormat.class,
				LongWritable.class,
				IteratorRecord.class,
				ctx.hadoopConfiguration()
			);
	}
	
	public JavaPairRDD<LongWritable,IteratorRecord> createScanRDD(String hdfsPath, Predicate... ps){
		queryConf.setFullScan(true);
		return createRDD(hdfsPath, ps);
	}
	
	public JavaPairRDD<LongWritable,IteratorRecord> createAdaptRDD(String hdfsPath, Predicate... ps){
		queryConf.setJustAccess(false);
		return createRDD(hdfsPath, ps);
	}

	public JavaPairRDD<LongWritable,IteratorRecord> createRepartitionRDD(String hdfsPath, Predicate... ps){
		queryConf.setRepartitionScan(true);
		return createRDD(hdfsPath, ps);
	}
}
