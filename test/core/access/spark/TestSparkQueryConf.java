package core.access.spark;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;

import core.access.Predicate;
import core.access.Predicate.PREDTYPE;
import core.utils.TypeUtils.*;

public class TestSparkQueryConf extends TestCase{

	SparkQueryConf sparkConf;
	String dummyDataset;
	int dummyWorkers;
	Predicate[] dummyPredicates;

	@Override
	public void setUp(){
		Configuration conf = new Configuration();
		sparkConf = new SparkQueryConf(conf);

		dummyDataset = "data123";
		dummyWorkers = 1;
		dummyPredicates = new Predicate[1];
		dummyPredicates[0] = new Predicate(0, TYPE.INT, 3002147, PREDTYPE.LEQ);
	}

	public void testDataset(){
		sparkConf.setDataset(dummyDataset);
		assertEquals(dummyDataset, sparkConf.getDataset());
	}

	public void testPredicates(){
		sparkConf.setPredicates(dummyPredicates);
		Predicate[] actualPredicates = sparkConf.getPredicates();
		for(int i=0; i<dummyPredicates.length; i++)
			assertEquals(dummyPredicates[i].toString(), actualPredicates[i].toString());
	}
}
