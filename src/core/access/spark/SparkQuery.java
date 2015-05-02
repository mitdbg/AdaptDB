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
		// ctx.hadoopConfiguration().setClass(FileInputFormat.PATHFILTER_CLASS, SparkPathFilter.class, PathFilter.class);
		return ctx.newAPIHadoopFile(
				new ConfUtils(queryConf.getCartilageProperties()).getHADOOP_NAMENODE() +  hdfsPath, 
				SparkInputFormat.class, 
				LongWritable.class, 
				IteratorRecord.class, 
				ctx.hadoopConfiguration()
			);
	}
}
