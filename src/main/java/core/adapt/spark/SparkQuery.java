package core.adapt.spark;

import core.adapt.Query;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import core.adapt.Predicate;
import core.adapt.iterator.IteratorRecord;
import core.utils.ConfUtils;

public class SparkQuery {
	protected SparkQueryConf queryConf;
	protected JavaSparkContext ctx;
	protected ConfUtils cfg;

	public SparkQuery(ConfUtils config) {
		this.cfg = config;
		SparkConf sconf = new SparkConf().setMaster(cfg.getSPARK_MASTER())
				.setAppName(this.getClass().getName())
				.setSparkHome(cfg.getSPARK_HOME())
				.setJars(new String[] { cfg.getSPARK_APPLICATION_JAR() })
				.set("spark.hadoop.cloneConf", "false")
				.set("spark.executor.memory", cfg.getSPARK_EXECUTOR_MEMORY())
				.set("spark.driver.memory", cfg.getSPARK_DRIVER_MEMORY())
				.set("spark.task.cpus", cfg.getSPARK_TASK_CPUS());

		ctx = new JavaSparkContext(sconf);
		ctx.hadoopConfiguration().setBoolean(
				FileInputFormat.INPUT_DIR_RECURSIVE, true);
		ctx.hadoopConfiguration().set("fs.hdfs.impl",
				org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		queryConf = new SparkQueryConf(ctx.hadoopConfiguration());
	}

	public JavaPairRDD<LongWritable, IteratorRecord> createRDD(String hdfsPath,
			Query q) {
		return this.createRDD(hdfsPath, 0, q);
	}

	public JavaPairRDD<LongWritable, IteratorRecord> createRDD(String hdfsPath,
			int replicaId, Query q) {
		queryConf.setWorkingDir(hdfsPath);
		queryConf.setReplicaId(replicaId);
		queryConf.setQuery(q);
		queryConf.setHadoopHome(cfg.getHADOOP_HOME());
		queryConf.setZookeeperHosts(cfg.getZOOKEEPER_HOSTS());
		queryConf.setMaxSplitSize(8589934592l); // 8gb is the max size for each
												// split (with 8 threads in
												// parallel)
		queryConf.setMinSplitSize(4294967296l); // 4gb
		queryConf.setHDFSReplicationFactor(cfg.getHDFS_REPLICATION_FACTOR());

		// TODO: This is tricky. Figure out how to do for multiple tables.
		return ctx.newAPIHadoopFile(cfg.getHADOOP_NAMENODE() + hdfsPath + "/"  + q.getTable() + "/data",
				SparkInputFormat.class, LongWritable.class,
				IteratorRecord.class, ctx.hadoopConfiguration());
	}

	public JavaPairRDD<LongWritable, IteratorRecord> createScanRDD(
			String hdfsPath, Query q) {
		queryConf.setFullScan(true);
		return createRDD(hdfsPath, q);
	}

	public JavaPairRDD<LongWritable, IteratorRecord> createAdaptRDD(
			String hdfsPath, Query q) {
		queryConf.setJustAccess(false);
		return createRDD(hdfsPath, q);
	}
}
