package core.access.spark;

import junit.framework.TestCase;
import perf.benchmark.BenchmarkSettings;
import core.adapt.Predicate;
import core.adapt.Predicate.PREDTYPE;
import core.adapt.Query.FilterQuery;
import core.adapt.iterator.PartitionIterator;
import core.adapt.iterator.PostFilterIterator;
import core.adapt.spark.SparkQuery;
import core.utils.ConfUtils;
import core.utils.TypeUtils.*;

public class TestQuery extends TestCase {
	public final static String propertyFile = BenchmarkSettings.conf;
	public final static ConfUtils cfg = new ConfUtils(propertyFile);

	SparkQuery sq;
	Predicate p;

	@Override
	public void setUp() {
		p = new Predicate(0, TYPE.INT, 30021470, PREDTYPE.LEQ);
		// sq = new SparkQuery(new Predicate[]{p}, cfg);
		// sampleFile = "hdfs://localhost:9000/user/alekh/cartilage.properties";
	}

	// public void testCreateTextFile(){
	// new SparkQuery(cfg).createTextFile(sampleFile, p);
	// }
	//
	// public void testCreateHadoopFile(){
	// new SparkQuery(cfg).createHadoopFile(sampleFile, p);
	// }
	//
	// public void testCreateHadoopRDD(){
	// new SparkQuery(cfg).createHadoopRDD(sampleFile, p);
	// }
	//
	// public void testCreateNewAPIHadoopRDD(){
	// new SparkQuery(cfg).createNewAPIHadoopRDD("/user/alekh/LICENSE.txt", p);
	// }

	public void testExecuteQuery() {
		long c = new SparkQuery(cfg).createRDD("/user/anil/smalltest", p)
				.count();
		System.out.println("Count = " + c);
	}

	public void testSerialize() {
		PostFilterIterator it = new PostFilterIterator(new FilterQuery(
				new Predicate[] { p }));
		String s = PartitionIterator.iteratorToString(it);
		assertNotNull(s);

		it = (PostFilterIterator) PartitionIterator.stringToIterator(s);
		assertNotNull(it);
	}

	public static void main(String[] args) {
		System.out.println("Zoomba");
		TestQuery tq = new TestQuery();
		tq.setUp();
		tq.testExecuteQuery();
	}
}
