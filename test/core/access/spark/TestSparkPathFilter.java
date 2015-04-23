package core.access.spark;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import core.access.Predicate;
import core.utils.RangeUtils;

public class TestSparkPathFilter extends TestCase{

	SparkPathFilter filter;
	Configuration conf;
	
	public void setUp(){
		filter = new SparkPathFilter();
		conf = new Configuration();
		
		SparkQueryConf sparkConf=  new SparkQueryConf(conf);
		Predicate[] predicates = new Predicate[]{
					new Predicate(0, RangeUtils.closed(1,10)), 
					new Predicate(1, RangeUtils.closed(1,10)), 
					new Predicate(2, RangeUtils.closed(1,10))
				};
		sparkConf.setPredicates(predicates);
		sparkConf.setDataset("data123");
	}

	public void testSetConf(){
		filter.setConf(conf);
		assert(true);
	}
	
	public void testAccept(){
		filter.setConf(conf);
		assertTrue(filter.accept(new Path("testpath")));
	}
}
