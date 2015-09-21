package perf.benchmark;

import org.apache.hadoop.io.LongWritable;
import org.apache.spark.api.java.JavaPairRDD;

import core.access.Predicate;
import core.access.iterator.IteratorRecord;
import core.access.spark.MapToKeyFunction;
import core.access.spark.join.SparkJoinQuery;
import core.utils.ConfUtils;
import core.utils.TypeUtils.TYPE;

public class TestJoinQueries {
	public final static String propertyFile = BenchmarkSettings.conf;
	public final static ConfUtils cfg = new ConfUtils(propertyFile);
	public final static int scaleFactor = 1000;
	public static int numQueries = 1;

	double selectivity = 0.05;
	SparkJoinQuery sq;

	public void setUp() {
		sq = new SparkJoinQuery(cfg);
	}

	public void testOrderLineitemJoin() {
		System.out.println("INFO: Running ORDERS, LINEITEM join Query");
		long start = System.currentTimeMillis();
		long result = sq.createJoinRDD(true, "/user/anil/orders", 0, 0,
				"/user/anil/repl", 1, 0).count();
		long end = System.currentTimeMillis();
		System.out.println("RES: ORDER-LINEITEM JOIN " + (end - start) + " "
				+ result);
	}

	public void testPartLineitemJoin() {
		System.out.println("INFO: Running PART, LINEITEM join Query");
		long start = System.currentTimeMillis();
		long result = sq.createJoinRDD(true, "/user/anil/part", 0, 0,
				"/user/anil/repl", 1, 1).count();
		long end = System.currentTimeMillis();
		System.out.println("RES: PART-LINEITEM JOIN " + (end - start) + " "
				+ result);
	}

	public void testScans() {
		System.out.println("INFO: Running ORDERS scan");
		long start = System.currentTimeMillis();
		long result = sq.createScanRDD(
				"/user/anil/orders",
				new Predicate[] { new Predicate(0, TYPE.LONG, -1L,
						Predicate.PREDTYPE.GT) }).count();
		long end = System.currentTimeMillis();
		System.out.println("RES: ORDERS scan " + (end - start) + " " + result);

		System.out.println("INFO: Running PART scan");
		long start2 = System.currentTimeMillis();
		long result2 = sq.createScanRDD(
				"/user/anil/part",
				new Predicate[] { new Predicate(0, TYPE.LONG, -1L,
						Predicate.PREDTYPE.GT) }).count();
		long end2 = System.currentTimeMillis();
		System.out.println("RES: PART scan " + (end2 - start2) + " " + result2);
	}

	public void testBaseline() {
		// JavaPairRDD<LongWritable, IteratorRecord> part =
		// sq.createScanRDD("/user/anil/original/part");
		JavaPairRDD<LongWritable, IteratorRecord> orders = sq
				.createScanRDD("/user/anil/orders/");
		JavaPairRDD<LongWritable, IteratorRecord> lineitem = sq
				.createScanRDD("/user/anil/repl/");

		long start = System.currentTimeMillis();
		JavaPairRDD<String, String> ordersKey = orders
				.mapToPair(new MapToKeyFunction(0));
		JavaPairRDD<String, String> lineKey = lineitem
				.mapToPair(new MapToKeyFunction(0));
		long result = lineKey.join(ordersKey).count();
		System.out.println("JOIN ORDERS-LINEITEM: time "
				+ (System.currentTimeMillis() - start) + " result " + result);
	}

	public void testSmallJoins() {
		String input1 = "/user/anil/part";
		String input2 = "/user/anil/repl";
		int numChunks = 10;
		int range = 200000 * 1000;
		int chunkSize = range / numChunks;
		double chunkSelectivity = selectivity / numChunks;
		long start = System.currentTimeMillis();
		long total = 0;
		for (int i = 0; i < numChunks; i++) {
			long lowVal = chunkSize
					* i
					+ (int) (Math.random() * chunkSize * (1 - chunkSelectivity));
			long highVal = lowVal + (int) (chunkSize * chunkSelectivity);
			System.out.println("INFO: getting partkeys in range " + lowVal
					+ " " + highVal);
			long startChunk = System.currentTimeMillis();
			JavaPairRDD<String, String> partRDD = sq
					.createRDD(
							input1,
							new Predicate(0, TYPE.LONG, lowVal,
									Predicate.PREDTYPE.GT),
							new Predicate(0, TYPE.LONG, highVal,
									Predicate.PREDTYPE.LEQ)).mapToPair(
							new MapToKeyFunction(0));

			JavaPairRDD<String, String> lineRDD = sq.createRDD(
					input2,
					new Predicate(1, TYPE.INT, (int) lowVal,
							Predicate.PREDTYPE.GT),
					new Predicate(1, TYPE.INT, (int) highVal,
							Predicate.PREDTYPE.LEQ)).mapToPair(
					new MapToKeyFunction(1));
			long result = partRDD.join(lineRDD).count();
			total += result;
			long endChunk = System.currentTimeMillis();
			System.out.println("RES: PART range join "
					+ (endChunk - startChunk) + " " + result);
		}
		long end = System.currentTimeMillis();
		System.out.println("RES: OVERALL PART range join " + (end - start)
				+ " " + total);
	}

	public static void main(String[] args) {
		TestJoinQueries tjq = new TestJoinQueries();
		tjq.setUp();
		// tjq.testOrderLineitemJoin();
		tjq.testPartLineitemJoin();
		// tjq.testBaseline();
		// tjq.testSmallJoins();
	}
}
