package core.access.spark;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;

import core.access.Predicate;
import core.utils.RangeUtils;

public class TestSparkQueryConf extends TestCase{

	SparkQueryConf sparkConf;	
	String dummyDataset;
	int dummyWorkers;
	Predicate[] dummyPredicates;
	
	public void setUp(){
		Configuration conf = new Configuration();
		sparkConf = new SparkQueryConf(conf);
		
		dummyDataset = "data123";
		dummyWorkers = 1;
		dummyPredicates = new Predicate[3];
		dummyPredicates[0] = new Predicate(0, RangeUtils.closed(1,10));
		dummyPredicates[1] = new Predicate(1, RangeUtils.closed(1,10));
		dummyPredicates[2] = new Predicate(2, RangeUtils.closed(1,10));
	}
	
	public void testDataset(){
		sparkConf.setDataset(dummyDataset);
		assertEquals(dummyDataset, sparkConf.getDataset());
	}
	
	public void testWorkers(){
		sparkConf.setWorkers(dummyWorkers);
		assertEquals(dummyWorkers, sparkConf.getWorkers());
	}
	
	public void testPredicates(){
		sparkConf.setPredicates(dummyPredicates);
		Predicate[] actualPredicates = sparkConf.getPredicates();
		for(int i=0; i<dummyPredicates.length; i++)
			assertEquals(dummyPredicates[i].toString(), actualPredicates[i].toString());
	}
}
