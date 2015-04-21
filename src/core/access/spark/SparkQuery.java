package core.access.spark;

import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import core.access.Predicate;
import core.access.iterator.PartitionIterator.IteratorRecord;
import core.utils.ConfUtils;

public class SparkQuery {

	public final static String propertyFile = "cartilage.properties";
	public final static ConfUtils cfg = new ConfUtils(propertyFile);
	
	private SparkQueryConf queryConf;
	private JavaSparkContext ctx;
	
	private Predicate[] predicates;
	
	public SparkQuery(Predicate[] predicates){
		this.predicates = predicates;
		ctx = new JavaSparkContext(cfg.getSPARK_MASTER(), this.getClass().getName(), cfg.getSPARK_HOME(), cfg.getSPARK_JAR());
		queryConf = new SparkQueryConf(ctx.hadoopConfiguration());
	}
	
	public JavaPairRDD<LongWritable,IteratorRecord> createRDD(String hdfsPath){
		queryConf.setDataset(hdfsPath);
		queryConf.setPredicates(predicates);
		queryConf.setWorkers(cfg.getNUM_RACKS() * cfg.getNODES_PER_RACK() * cfg.getMAP_TASKS());
		ctx.hadoopConfiguration().setClass(FileInputFormat.PATHFILTER_CLASS, SparkPathFilter.class, PathFilter.class);
		return ctx.newAPIHadoopFile(hdfsPath, SparkInputFormat.class, LongWritable.class, IteratorRecord.class, ctx.hadoopConfiguration());
	}
	
}
