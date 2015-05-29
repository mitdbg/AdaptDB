package core.access.benchmark;

import junit.framework.TestCase;
import core.access.Predicate;
import core.access.Predicate.PREDTYPE;
import core.access.spark.SparkQuery;
import core.index.Settings;
import core.utils.ConfUtils;
import core.utils.RangeUtils.SimpleDateRange.SimpleDate;
import core.utils.SchemaUtils.TYPE;

public class SingleAttributeQueries extends TestCase{
	public final static String propertyFile = Settings.cartilageConf;
	public final static ConfUtils cfg = new ConfUtils(propertyFile);

	@Override
	public void setUp() {

	}

	public void testShipDateQueries(){
		int numQueries = 1;
		SparkQuery sq = new SparkQuery(cfg);
		for (int i=1; i <= numQueries; i++) {
			int year = 1993 + (i + 1) % 5;
			System.out.println("MDINDEX: Running Query " + i);
			Predicate p1 = new Predicate(10, TYPE.DATE, new SimpleDate(year-1,12,31), PREDTYPE.GT);
			long result = sq.createRDD(Settings.hdfsPartitionDir, p1).count();
			System.out.println(result);
		}
	}

	public void testOrderKeyQueries(){
		int numQueries = 5;
		SparkQuery sq = new SparkQuery(cfg);
		for (int i=1; i <= numQueries; i++) {
			int orderKey = (int) (Math.random() * 187500) * 32;
			Predicate p1 = new Predicate(0, TYPE.INT, orderKey, PREDTYPE.EQ);
			long result = sq.createRDD(Settings.hdfsPartitionDir, p1).count();
			System.out.println("orderkey = "+orderKey+": "+result);
		}
		// sometimes this counts 0 even though the orderkey is in the table - why?
	}

	public void testPartKeyQueries(){
		int numQueries = 5;
		SparkQuery sq = new SparkQuery(cfg);
		for (int i=1; i <= numQueries; i++) {
			int partKey = (int) (Math.random() * 200000);
			Predicate p1 = new Predicate(1, TYPE.INT, partKey, PREDTYPE.EQ);
			long result = sq.createRDD(Settings.hdfsPartitionDir, p1).count();
			System.out.println("partkey = "+partKey+": "+result);
		}
	}

	public void testSupplierKeyQueries(){
		int numQueries = 5;
		SparkQuery sq = new SparkQuery(cfg);
		for (int i=1; i <= numQueries; i++) {
			int suppKey = (int) (Math.random() * 10000);
			Predicate p1 = new Predicate(2, TYPE.INT, suppKey, PREDTYPE.EQ);
			long result = sq.createRDD(Settings.hdfsPartitionDir, p1).count();
			System.out.println("supplierkey = "+suppKey+": "+result);
		}
	}

	// no queries with selection on linenumber

	public void testQuantityQueries(){
		int numQueries = 5;
		SparkQuery sq = new SparkQuery(cfg);
		for (int i=0; i < numQueries; i++) {
			Predicate p1 = new Predicate(4, TYPE.INT, i*10, PREDTYPE.GT);
			//Predicate p2 = new Predicate(4, TYPE.INT, (i+1)*10, PREDTYPE.LEQ);
			long result = sq.createRDD(Settings.hdfsPartitionDir, p1).count();
			System.out.println("quantity "+(i*10)+"-"+(i*10+10)+": "+result);
		}
	}

	// no queries with selection on extended price

	public void testDiscountQueries(){
		int numQueries = 9;
		SparkQuery sq = new SparkQuery(cfg);
		for (int i=0; i < numQueries; i++) {
			double discount = i * .01 + .02;
			Predicate p1 = new Predicate(6, TYPE.FLOAT, discount, PREDTYPE.GT);
			Predicate p2 = new Predicate(6, TYPE.FLOAT, discount+.01, PREDTYPE.LEQ);
			long result = sq.createRDD(Settings.hdfsPartitionDir, p1, p2).count();
			System.out.println("discount "+discount+"-"+(discount+.01)+": "+result);
		}
		// also, > 0 is only 737555 results, not ~811000 (but both are too low)
	}

	// no queries with selection on tax

	public void testReturnFlagQuery(){
		SparkQuery sq = new SparkQuery(cfg);
		Predicate p1 = new Predicate(8, TYPE.STRING, "R", PREDTYPE.EQ);
		long result = sq.createRDD(Settings.hdfsPartitionDir, p1).count();
		System.out.println("return flag = R: "+result);
	}

	// no queries with selection on linestatus
	// shipdate already tested
	// commitdate only selecting for commitdate > receiptdate

	public void testReceiptDateQueries(){
		SparkQuery sq = new SparkQuery(cfg);
		int numQueries = 5;
		for (int i=0; i < numQueries; i++) {
			int year = 1993 + i % 5;
			Predicate p1 = new Predicate(12, TYPE.DATE, new SimpleDate(year,1,1), PREDTYPE.GEQ);
			long result = sq.createRDD(Settings.hdfsPartitionDir, p1).count();
			System.out.println("receipt date >= 1/1/"+year+": "+result);
		}
	}

	public void testShipInstructQuery(){
		SparkQuery sq = new SparkQuery(cfg);
		Predicate p1 = new Predicate(13, TYPE.STRING, "DELIVER IN PERSON", PREDTYPE.EQ);
		long result = sq.createRDD(Settings.hdfsPartitionDir, p1).count();
		System.out.println("shipinstruct = DELIVER IN PERSON: "+result);
	}

	public void testShipModeQuery(){
		SparkQuery sq = new SparkQuery(cfg);
		Predicate p1 = new Predicate(14, TYPE.STRING, "AIR", PREDTYPE.EQ);
		long result = sq.createRDD(Settings.hdfsPartitionDir, p1).count();
		System.out.println("shipmode = AIR: "+result);
	}

	// no queries on comment

	public static void main(String[] args) {
		System.out.println("Started BAZINGA");
		SingleAttributeQueries saq = new SingleAttributeQueries();
		saq.setUp();
		saq.testShipDateQueries();
	}
}
