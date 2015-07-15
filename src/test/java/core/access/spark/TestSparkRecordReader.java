package core.access.spark;

import java.io.IOException;

import junit.framework.TestCase;
import perf.benchmark.BenchmarkSettings;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import core.access.Predicate;
import core.access.Predicate.PREDTYPE;
import core.access.iterator.IteratorRecord;
import core.utils.ConfUtils;
import core.utils.TypeUtils.*;

public class TestSparkRecordReader extends TestCase{

	Configuration conf;
	Job job;
	
	
	public void setUp(){
		
		// query predicate and input data path
		Predicate[] predicates = new Predicate[]{new Predicate(0, TYPE.INT, 3002147, PREDTYPE.LEQ)};
		String hdfsPath = "hdfs://localhost:9000/user/alekh/dodo";
		
		
		conf = new Configuration();
		ConfUtils cfg = new ConfUtils(BenchmarkSettings.cartilageConf);
		
		SparkQueryConf queryConf = new SparkQueryConf(conf);
		queryConf.setWorkingDir(hdfsPath);
		queryConf.setPredicates(predicates);
		queryConf.setHadoopHome(cfg.getHADOOP_HOME());
		queryConf.setZookeeperHosts(cfg.getZOOKEEPER_HOSTS());
		queryConf.setMaxSplitSize(1024 / 64);
		
		try {
			job = Job.getInstance(conf);
			FileInputFormat.setInputPaths(job, hdfsPath);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public void testNextKeyValue(){
		int count = 0;
		
		try {
			SparkInputFormat sparkInputFormat = new SparkInputFormat();
			for(InputSplit split: sparkInputFormat.getSplits(job)){			
				RecordReader<LongWritable, IteratorRecord> recordReader = sparkInputFormat.createRecordReader(split, null);				
				TaskAttemptContext ctx = new TaskAttemptContextImpl(conf, new TaskAttemptID());
				recordReader.initialize(split, ctx);
				while(recordReader.nextKeyValue()){
					recordReader.getCurrentKey();
					recordReader.getCurrentValue();
					count++;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		System.out.println("Output record count = "+count);
	}
	
}
