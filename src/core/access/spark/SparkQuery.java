package core.access.spark;

import org.apache.hadoop.io.LongWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import core.access.Predicate;
import core.access.iterator.PartitionIterator.IteratorRecord;
import core.utils.ConfUtils;

public class SparkQuery {

	private SparkQueryConf queryConf;
	private JavaSparkContext ctx;

	private Predicate[] predicates;

	private ConfUtils cfg;

	public SparkQuery(Predicate[] predicates, ConfUtils config) {
		this.predicates = predicates;
		this.cfg = config;
		ctx = new JavaSparkContext(cfg.getSPARK_MASTER(), this.getClass().getName());
		//ctx = new JavaSparkContext(cfg.getSPARK_MASTER(), this.getClass().getName(), cfg.getSPARK_HOME(), cfg.getSPARK_JAR());
		queryConf = new SparkQueryConf(ctx.hadoopConfiguration());
	}

	public JavaPairRDD<LongWritable,IteratorRecord> createRDD(String hdfsPath){
		queryConf.setDataset(hdfsPath);
		queryConf.setPredicates(predicates);
		queryConf.setWorkers(cfg.getNUM_RACKS() * cfg.getNODES_PER_RACK() * cfg.getMAP_TASKS());
		queryConf.setHadoopHome(cfg.getHADOOP_HOME());
		queryConf.setZookeeperHosts(cfg.getZOOKEEPER_HOSTS());
		queryConf.setMaxSplitSize(1024 / 64);	// number of 64 MB partitions that can fit for each worker (we assume 1GB memory for each worker)
		// ctx.hadoopConfiguration().setClass(FileInputFormat.PATHFILTER_CLASS, SparkPathFilter.class, PathFilter.class);
		
		System.setProperty("spark.executor.memory","4g");
		System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//		System.setProperty("spark.serializer", "spark.KryoSerializer");
//		System.setProperty("spark.kryo.registrator", "mypackage.MyRegistrator");
		
		return ctx.newAPIHadoopFile(
				cfg.getHADOOP_NAMENODE() +  hdfsPath, 
				SparkInputFormat.class, 
				LongWritable.class, 
				IteratorRecord.class, 
				ctx.hadoopConfiguration()
			);
	}
}
