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

	SparkQuery sq;
	
	@Override
	public void setUp() {
		sq = new SparkQuery(cfg);
	}

	public void testShipDateQueries(){
		int numQueries = 5;
		for (int i=1; i <= numQueries; i++) {
			System.out.println("INFO: Running SHIPDATE Query " + i);
			int year = 1993 + (i + 1) % 5;
			long start = System.currentTimeMillis();
			Predicate p1 = new Predicate(10, TYPE.DATE, new SimpleDate(year-1,12,31), PREDTYPE.GT);
			Predicate p2 = new Predicate(10, TYPE.DATE, new SimpleDate(year,12,31), PREDTYPE.LEQ);			
			long result = sq.createRDD(Settings.hdfsPartitionDir, p1, p2).count();
			long end = System.currentTimeMillis();
			System.out.println("RES: SHIPDATE " + (end - start) + " " + result);
		}
	}

	public void testOrderKeyQueries(){
		int numQueries = 5;
		for (int i=1; i <= numQueries; i++) {
			System.out.println("INFO: Running ORDERKEY Query " + i);
			long orderKey = (long) (Math.random() * 187500) * 32;
			long start = System.currentTimeMillis();
			Predicate p1 = new Predicate(0, TYPE.LONG, orderKey, PREDTYPE.EQ);
			long result = sq.createRDD(Settings.hdfsPartitionDir, p1).count();
			long end = System.currentTimeMillis();
			System.out.println("RES: ORDERKEY " + (end - start) + " " + orderKey + " " + result);		
		}
		// sometimes this counts 0 even though the orderkey is in the table - why?
	}

	public void testPartKeyQueries(){
		int numQueries = 1;
		for (int i=1; i <= numQueries; i++) {
			System.out.println("INFO: Running PARTKEY Query " + i);
			int partKey = (int) (Math.random() * 200000);
			long start = System.currentTimeMillis();
			Predicate p1 = new Predicate(1, TYPE.INT, partKey, PREDTYPE.EQ);
			long result = sq.createRDD(Settings.hdfsPartitionDir, p1).count();
			long end = System.currentTimeMillis();
			System.out.println("RES: PARTKEY " + (end - start) + " " + partKey + " " + result);
		}
	}

	public void testSupplierKeyQueries(){
		int numQueries = 1;
		for (int i=1; i <= numQueries; i++) {
			System.out.println("INFO: Running SUPPLIERKEY Query " + i);
			int suppKey = (int) (Math.random() * 10000);
			long start = System.currentTimeMillis();
			Predicate p1 = new Predicate(2, TYPE.INT, suppKey, PREDTYPE.EQ);
			long result = sq.createRDD(Settings.hdfsPartitionDir, p1).count();
			long end = System.currentTimeMillis();
			System.out.println("RES: SUPPLIERKEY " + (end - start) + " " + suppKey + " " + result);
		}
	}

	// no queries with selection on linenumber

	public void testQuantityQueries(){
		int numQueries = 1;
		for (int i=0; i < numQueries; i++) {
			System.out.println("INFO: Running QUANTITY Query " + i);
			Predicate p1 = new Predicate(4, TYPE.INT, i*10, PREDTYPE.GT);
			Predicate p2 = new Predicate(4, TYPE.INT, (i+1)*10, PREDTYPE.LEQ);
			long start = System.currentTimeMillis();
			long result = sq.createRDD(Settings.hdfsPartitionDir, p1, p2).count();
			long end = System.currentTimeMillis();
			System.out.println("RES: QUANTITY " + (end - start) + " " + (i*10)+"-"+(i*10+10) + " " + result);
		}
	}

	// no queries with selection on extended price

	public void testDiscountQueries(){
		int numQueries = 1;
		for (int i=0; i < numQueries; i++) {
			double discount = i * .01 + .02;
			System.out.println("INFO: Running DISCOUNT Query " + i);
			Predicate p1 = new Predicate(6, TYPE.FLOAT, discount, PREDTYPE.GT);
			Predicate p2 = new Predicate(6, TYPE.FLOAT, discount+.01, PREDTYPE.LEQ);
			long start = System.currentTimeMillis();
			long result = sq.createRDD(Settings.hdfsPartitionDir, p1, p2).count();
			long end = System.currentTimeMillis();
			System.out.println("RES: DISCOUNT " + (end - start) + " " + discount+"-"+(discount+.01) + " " + result);
		}
		// also, > 0 is only 737555 results, not ~811000 (but both are too low)
	}

	// no queries with selection on tax

	public void testReturnFlagQuery(){
		System.out.println("INFO: Running RETURN FLAG Query " + 1);
		Predicate p1 = new Predicate(8, TYPE.STRING, "R", PREDTYPE.EQ);
		long start = System.currentTimeMillis();
		long result = sq.createRDD(Settings.hdfsPartitionDir, p1).count();
		long end = System.currentTimeMillis();
		System.out.println("RES: RETURN FLAG " + (end - start) + " " + "R" + " " + result);
	}

	// no queries with selection on linestatus
	// shipdate already tested
	// commitdate only selecting for commitdate > receiptdate

	public void testReceiptDateQueries(){
		int numQueries = 1;
		for (int i=0; i < numQueries; i++) {
			int year = 1993 + i % 5;
			System.out.println("INFO: Running RECIEPT DATE Query " + i);
			Predicate p1 = new Predicate(10, TYPE.DATE, new SimpleDate(year-1,12,31), PREDTYPE.GT);
			Predicate p2 = new Predicate(10, TYPE.DATE, new SimpleDate(year,12,31), PREDTYPE.LEQ);
			long start = System.currentTimeMillis();
			long result = sq.createRDD(Settings.hdfsPartitionDir, p1, p2).count();
			long end = System.currentTimeMillis();
			System.out.println("RES: RECIEPT DATE " + (end - start) + " " + year + " " + result);
		}
	}

	public void testShipInstructQuery(){
		Predicate p1 = new Predicate(13, TYPE.STRING, "DELIVER IN PERSON", PREDTYPE.EQ);
		System.out.println("INFO: Running SHIP INSTRUCT Query " + 1);
		long start = System.currentTimeMillis();
		long result = sq.createRDD(Settings.hdfsPartitionDir, p1).count();
		long end = System.currentTimeMillis();
		System.out.println("RES: SHIP INSTRUCT " + (end - start) + " " + "DELIVER IN PERSON" + " " + result);
	}

	public void testShipModeQuery(){
		System.out.println("INFO: Running SHIP MODE Query " + 1);
		Predicate p1 = new Predicate(14, TYPE.STRING, "AIR", PREDTYPE.EQ);
		long start = System.currentTimeMillis();
		long result = sq.createRDD(Settings.hdfsPartitionDir, p1).count();
		long end = System.currentTimeMillis();
		System.out.println("RES: SHIP MODE " + (end - start) + " " + "AIR" + " " + result);
	}

	// no queries on comment

	public static void main(String[] args) {
		System.out.println("Started BAZINGA");
		SingleAttributeQueries saq = new SingleAttributeQueries();
		saq.setUp();
//		saq.testShipDateQueries();
//		saq.testOrderKeyQueries();
		saq.testPartKeyQueries();
		saq.testSupplierKeyQueries();
		saq.testQuantityQueries();
		saq.testDiscountQueries();
		saq.testReturnFlagQuery();
		saq.testReceiptDateQueries();
		saq.testShipInstructQuery();
		saq.testShipModeQuery();
	}
}
