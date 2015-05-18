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

	public void testSinglePredicateQueries(){
		int numQueries = 5;
		for (int i=1; i <= numQueries; i++) {
			int year = 1993 + (i + 1) % 5;
			System.out.println("MDINDEX: Running Query " + i);
			Predicate p1 = new Predicate(10, TYPE.DATE, new SimpleDate(year-1,12,31), PREDTYPE.GT);
			SparkQuery sq = new SparkQuery(new Predicate[]{p1}, cfg);
			System.out.println(sq.createRDD(Settings.hdfsPartitionDir).count());
			//numQueries--;
			sq.stopContext();
		}
	}

	public void testOrderKeyQueries(){
		int numQueries = 5;
		for (int i=1; i <= numQueries; i++) {
			int orderKey = (int) (Math.random() * 187500) * 32;
			Predicate p1 = new Predicate(0, TYPE.INT, orderKey, PREDTYPE.EQ);
			SparkQuery sq = new SparkQuery(new Predicate[]{p1}, cfg);
			System.out.println("orderkey = "+orderKey+": "+sq.createRDD(Settings.hdfsPartitionDir).count());
			sq.stopContext();
		}
		// sometimes this counts 0 even though the orderkey is in the table - why?
	}

	public void testPartKeyQueries(){
		int numQueries = 5;
		for (int i=1; i <= numQueries; i++) {
			int partKey = (int) (Math.random() * 200000);
			Predicate p1 = new Predicate(1, TYPE.INT, partKey, PREDTYPE.EQ);
			SparkQuery sq = new SparkQuery(new Predicate[]{p1}, cfg);
			System.out.println("partkey = "+partKey+": "+sq.createRDD(Settings.hdfsPartitionDir).count());
			sq.stopContext();
		}
	}

	public void testSupplierKeyQueries(){
		int numQueries = 5;
		for (int i=1; i <= numQueries; i++) {
			int suppKey = (int) (Math.random() * 10000);
			Predicate p1 = new Predicate(2, TYPE.INT, suppKey, PREDTYPE.EQ);
			SparkQuery sq = new SparkQuery(new Predicate[]{p1}, cfg);
			System.out.println("supplierkey = "+suppKey+": "+sq.createRDD(Settings.hdfsPartitionDir).count());
			sq.stopContext();
		}
	}

	// no queries with selection on linenumber

	public void testQuantityQueries(){
		int numQueries = 5;
		for (int i=0; i < numQueries; i++) {
			Predicate p1 = new Predicate(4, TYPE.INT, i*10, PREDTYPE.GT);
			Predicate p2 = new Predicate(4, TYPE.INT, (i+1)*10, PREDTYPE.LEQ);
			SparkQuery sq = new SparkQuery(new Predicate[]{p1, p2}, cfg);
			System.out.println("quantity "+(i*10)+"-"+(i*10+10)+": "+sq.createRDD(Settings.hdfsPartitionDir).count());
			sq.stopContext();
		}
	}

	// no queries with selection on extended price

	public void testDiscountQueries(){
		int numQueries = 9;
		for (int i=0; i < numQueries; i++) {
			double discount = i * .01 + .02;
			Predicate p1 = new Predicate(6, TYPE.FLOAT, discount, PREDTYPE.GT);
			Predicate p2 = new Predicate(6, TYPE.FLOAT, discount+.01, PREDTYPE.LEQ);
			SparkQuery sq = new SparkQuery(new Predicate[]{p1, p2}, cfg);
			System.out.println("discount "+discount+"-"+(discount+.01)+": "+sq.createRDD(Settings.hdfsPartitionDir).count());
			sq.stopContext();
		}
		// also, > 0 is only 737555 results, not ~811000 (but both are too low)
	}

	// no queries with selection on tax

	public void testReturnFlagQuery(){
		Predicate p1 = new Predicate(8, TYPE.STRING, "R", PREDTYPE.EQ);
		SparkQuery sq = new SparkQuery(new Predicate[]{p1}, cfg);
		System.out.println("return flag = R: "+sq.createRDD(Settings.hdfsPartitionDir).count());
		sq.stopContext();
	}

	// no queries with selection on linestatus
	// shipdate already tested
	// commitdate only selecting for commitdate > receiptdate

	public void testReceiptDateQueries(){
		int numQueries = 5;
		for (int i=0; i < numQueries; i++) {
			int year = 1993 + i % 5;
			Predicate p1 = new Predicate(12, TYPE.DATE, new SimpleDate(year,1,1), PREDTYPE.GEQ);
			SparkQuery sq = new SparkQuery(new Predicate[]{p1}, cfg);
			System.out.println("receipt date >= 1/1/"+year+": "+sq.createRDD(Settings.hdfsPartitionDir).count());
			sq.stopContext();
		}
	}

	public void testShipInstructQuery(){
		Predicate p1 = new Predicate(13, TYPE.STRING, "DELIVER IN PERSON", PREDTYPE.EQ);
		SparkQuery sq = new SparkQuery(new Predicate[]{p1}, cfg);
		System.out.println("shipinstruct = DELIVER IN PERSON: "+sq.createRDD(Settings.hdfsPartitionDir).count());
		sq.stopContext();
	}

	public void testShipModeQuery(){
		Predicate p1 = new Predicate(14, TYPE.STRING, "AIR", PREDTYPE.EQ);
		SparkQuery sq = new SparkQuery(new Predicate[]{p1}, cfg);
		System.out.println("shipmode = AIR: "+sq.createRDD(Settings.hdfsPartitionDir).count());
		sq.stopContext();
	}

	// no queries on comment
}
