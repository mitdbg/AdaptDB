package core.access.spark;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import core.access.Predicate;
import core.access.iterator.IteratorRecord;
import core.index.key.Schema;
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
				.set("spark.executor.memory", "150g")
				.set("spark.driver.memory", "10g").set("spark.task.cpus", "64");

		ctx = new JavaSparkContext(sconf);
		ctx.hadoopConfiguration().setBoolean(
				FileInputFormat.INPUT_DIR_RECURSIVE, true);
		ctx.hadoopConfiguration().set("fs.hdfs.impl",
				org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		queryConf = new SparkQueryConf(ctx.hadoopConfiguration());
	}

	public JavaPairRDD<LongWritable, IteratorRecord> createRDD(String hdfsPath,
			Predicate... ps) {
		// TODO(anil): When no replica is specified; figure out which replica
		// to run it off.
		return this.createRDD(hdfsPath, 0, ps);
	}

	public JavaPairRDD<LongWritable, IteratorRecord> createRDD(String hdfsPath,
			int replicaId, Predicate... ps) {
		queryConf.setWorkingDir(hdfsPath);
		queryConf.setReplicaId(replicaId);
		queryConf.setPredicates(ps);
		queryConf.setHadoopHome(cfg.getHADOOP_HOME());
		queryConf.setZookeeperHosts(cfg.getZOOKEEPER_HOSTS());
		queryConf.setMaxSplitSize(8589934592l); // 8gb is the max size for each
												// split (with 8 threads in
												// parallel)
		queryConf.setMinSplitSize(4294967296l); // 4gb
		queryConf.setHDFSReplicationFactor(cfg.getHDFS_REPLICATION_FACTOR());
		queryConf.setSchema(Schema.schema.toString());

		return ctx.newAPIHadoopFile(cfg.getHADOOP_NAMENODE() + hdfsPath + "/"
				+ replicaId, SparkInputFormat.class, LongWritable.class,
				IteratorRecord.class, ctx.hadoopConfiguration());
	}

	public JavaPairRDD<LongWritable, IteratorRecord> createScanRDD(
			String hdfsPath, Predicate... ps) {
		queryConf.setFullScan(true);
		return createRDD(hdfsPath, ps);
	}

	public JavaPairRDD<LongWritable, IteratorRecord> createAdaptRDD(
			String hdfsPath, Predicate... ps) {
		queryConf.setJustAccess(false);
		return createRDD(hdfsPath, ps);
	}

	public JavaPairRDD<LongWritable, IteratorRecord> createRepartitionRDD(
			String hdfsPath, Predicate... ps) {
		queryConf.setRepartitionScan(true);
		return createRDD(hdfsPath, ps);
	}
}
