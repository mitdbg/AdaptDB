package core.adapt.spark;

import core.adapt.Query;
import core.adapt.spark.join.SparkJoinInputFormat;
import core.utils.RangePartitionerUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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

	private String Delimiter = "|";

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

		try {
			sconf.registerKryoClasses(new Class<?>[]{
					Class.forName("org.apache.hadoop.io.LongWritable"),
					Class.forName("org.apache.hadoop.io.Text")
			});
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

		ctx = new JavaSparkContext(sconf);
		ctx.hadoopConfiguration().setBoolean(
				FileInputFormat.INPUT_DIR_RECURSIVE, true);
		ctx.hadoopConfiguration().set("fs.hdfs.impl",
				org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		queryConf = new SparkQueryConf(ctx.hadoopConfiguration());
	}

	public void setDelimiter(String delimiter){
		Delimiter = delimiter;
	}

	public String getDelimiter(){
		return Delimiter;
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


	/* SparkJoinQuery */

	public JavaPairRDD<LongWritable, Text> createJoinRDD(String hdfsPath) {
		return this.createJoinRDD(hdfsPath, 0);
	}

	public JavaPairRDD<LongWritable, Text> createJoinRDD(String hdfsPath,
														 int replicaId) {
		queryConf.setWorkingDir(hdfsPath);
		queryConf.setReplicaId(replicaId);

		queryConf.setHadoopHome(cfg.getHADOOP_HOME());
		queryConf.setZookeeperHosts(cfg.getZOOKEEPER_HOSTS());
		queryConf.setMaxSplitSize(8589934592l); // 8gb is the max size for each
		// split (with 8 threads in
		// parallel)
		queryConf.setMinSplitSize(4294967296l); // 4gb
		queryConf.setHDFSReplicationFactor(cfg.getHDFS_REPLICATION_FACTOR());


		System.out.println("PAHT: " + cfg.getHADOOP_NAMENODE() + hdfsPath);

		return ctx.newAPIHadoopFile(cfg.getHADOOP_NAMENODE() + hdfsPath,
				SparkJoinInputFormat.class, LongWritable.class,
				Text.class, ctx.hadoopConfiguration());
	}


	public String getCutPoints(String dataset, int attr){
		int[] cutpoints = {0, 1000000, 2000000, 3000000, 4000000,5000000};

		return RangePartitionerUtils.getStringCutPoints(cutpoints);
	}

	public JavaPairRDD<LongWritable, Text> createJoinScanRDD(
			String dataset1, String dataset1_schema, Query dataset1_query, int join_attr1, String dataset1_cutpoints,
			String dataset2, String dataset2_schema, Query dataset2_query, int join_attr2, String dataset2_cutpoints,
			int budget) {
		// type == 0, mdindex, == 1 raw files

		if(join_attr1 == -1 || join_attr2 == -1) {
			throw new RuntimeException("Join Attrs cannot be -1.");
		}

		Configuration conf = ctx.hadoopConfiguration();

		conf.set("DATASET1", dataset1);
		conf.set("DATASET2", dataset2);

		conf.set("DATASET1_CUTPOINTS", dataset1_cutpoints);
		conf.set("DATASET2_CUTPOINTS", dataset2_cutpoints);

		conf.set("DATASET1_SCHEMA", dataset1_schema);
		conf.set("DATASET2_SCHEMA", dataset2_schema);

		conf.set("DATASET1_QUERY", dataset1_query.toString());
		conf.set("DATASET2_QUERY", dataset2_query.toString());

		conf.set("JOIN_ATTR1", Integer.toString(join_attr1));
		conf.set("JOIN_ATTR2", Integer.toString(join_attr2));

		conf.set("BUDGET", Integer.toString(budget));

		conf.set("DELIMITER", Delimiter);

		String hdfsPath = cfg.getHDFS_WORKING_DIR();

		queryConf.setFullScan(true);
		return createJoinRDD(hdfsPath);
	}



	public JavaPairRDD<LongWritable, Text> createJoinScanRDD(
			String hdfsPath) {
		queryConf.setFullScan(true);
		return createJoinRDD(hdfsPath);
	}

	public JavaPairRDD<LongWritable, Text> createJoinAdaptRDD(
			String hdfsPath) {
		queryConf.setJustAccess(false);
		return createJoinRDD(hdfsPath);
	}

}
