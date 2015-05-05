package core.opt.simulator;

import java.io.IOException;
import java.util.List;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import core.access.Predicate;
import core.access.Predicate.PREDTYPE;
import core.access.spark.SparkInputFormat;
import core.access.spark.SparkQueryConf;
import core.index.Settings;
import core.utils.ConfUtils;
import core.utils.SchemaUtils.TYPE;

public class Simulator extends TestCase{
	SparkInputFormat sparkInputFormat;
	SparkQueryConf queryConf;
	String hdfsPath;
	Job job;

	@Override
	public void setUp(){
		hdfsPath = "hdfs://localhost:9000/user/anil/dodo";

		Configuration conf = new Configuration();
		ConfUtils cfg = new ConfUtils(Settings.cartilageConf);

		queryConf = new SparkQueryConf(conf);
		queryConf.setDataset(hdfsPath);
		queryConf.setWorkers(cfg.getNUM_RACKS() * cfg.getNODES_PER_RACK() * cfg.getMAP_TASKS());
		queryConf.setHadoopHome(cfg.getHADOOP_HOME());
		queryConf.setZookeeperHosts(cfg.getZOOKEEPER_HOSTS());
		queryConf.setMaxSplitSize(1024 / 64);
	}

	public void testGetSplits(){
		Predicate[] predicates = new Predicate[]{new Predicate(0, TYPE.INT, 3002147, PREDTYPE.LEQ)};
		queryConf.setPredicates(predicates);
		try {
			job = Job.getInstance(queryConf.getConf());
			FileInputFormat.setInputPaths(job, hdfsPath);
			sparkInputFormat = new SparkInputFormat();
			List<InputSplit> splits = sparkInputFormat.getSplits(job);
			System.out.println(splits.size());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void testCreateRecordReader(){
		try {
			sparkInputFormat = new SparkInputFormat();
			List<InputSplit> splits = sparkInputFormat.getSplits(job);
			for(InputSplit split: splits)
				sparkInputFormat.createRecordReader(split, null);

		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
