package core.access.spark;

import junit.framework.TestCase;
import core.access.Predicate;
import core.access.Predicate.PREDTYPE;
import core.access.Query.FilterQuery;
import core.access.iterator.PartitionIterator;
import core.access.iterator.PostFilterIterator;
import core.index.Settings;
import core.utils.ConfUtils;
import core.utils.SchemaUtils.TYPE;

public class TestQuery extends TestCase{
	public final static String propertyFile = Settings.cartilageConf;
	public final static ConfUtils cfg = new ConfUtils(propertyFile);

	SparkQuery sq;
	Predicate p;

	String sampleFile;

	@Override
	public void setUp() {
		p = new Predicate(0, TYPE.INT, 30021470, PREDTYPE.LEQ);
		//sq = new SparkQuery(new Predicate[]{p}, cfg);
		sampleFile = "hdfs://localhost:9000/user/alekh/cartilage.properties";
	}


	public void testCreateTextFile(){
		new SparkQuery(new Predicate[]{p}, cfg).createTextFile(sampleFile);
	}

	public void testCreateHadoopFile(){
		new SparkQuery(new Predicate[]{p}, cfg).createHadoopFile(sampleFile);
	}

	public void testCreateHadoopRDD(){
		new SparkQuery(new Predicate[]{p}, cfg).createHadoopRDD(sampleFile);
	}

	public void testCreateNewAPIHadoopRDD(){
		new SparkQuery(new Predicate[]{p}, cfg).createNewAPIHadoopRDD("/user/alekh/LICENSE.txt");
	}

	public void testExecuteQuery(){
		long c = new SparkQuery(new Predicate[]{p}, cfg).createRDD("/user/anil/dodo").count();
		System.out.println("Count = "+c);
	}

	public void testSerialize(){
		PostFilterIterator it = new PostFilterIterator(new FilterQuery(new Predicate[]{p}));
		String s = PartitionIterator.iteratorToString(it);
		assertNotNull(s);

		it = (PostFilterIterator)PartitionIterator.stringToIterator(s);
		assertNotNull(it);
	}

	public static void main(String[] args) {
	}
}
