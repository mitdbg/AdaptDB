package perf.benchmark;

import java.io.BufferedReader;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Random;

import org.apache.curator.framework.CuratorFramework;

import core.access.Predicate;
import core.access.Predicate.PREDTYPE;
import core.access.spark.SparkQuery;
import core.utils.ConfUtils;
import core.utils.CuratorUtils;
import core.utils.HDFSUtils;
import core.utils.TypeUtils.SimpleDate;
import core.utils.TypeUtils.TYPE;

public class TestChangingWorkload {
	public final static String propertyFile = BenchmarkSettings.cartilageConf;
	public final static ConfUtils cfg = new ConfUtils(propertyFile);
	final static String[] shipModes = new String[]{"REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"};

	int scaleFactor = 1000;
	double selectivity = 0.05;

	public void setUp() {
		// delete query history
		// Cleanup queries file - to remove past query workload
		HDFSUtils.deleteFile(HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME()),
				cfg.getHDFS_WORKING_DIR() + "/queries", false);

		// reset all the bucket counts
		CuratorFramework client = CuratorUtils.createAndStartClient(cfg.getZOOKEEPER_HOSTS());
		CuratorUtils.deleteAll(client, "/", "partition-");

		Charset charset = Charset.forName("US-ASCII");
		Path file = FileSystems.getDefault().getPath("/data/mdindex/tpch-dbgen/buckets");
//		Path file = FileSystems.getDefault().getPath("/Users/qui/Documents/buckets.txt");
		try {
			BufferedReader reader = Files.newBufferedReader(file, charset);
			String line = null;
			while ((line = reader.readLine()) != null) {
				String[] tokens = line.split("\t");
				CuratorUtils.setCounter(client, tokens[0], Integer.parseInt(tokens[1]));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		CuratorUtils.stopClient(client);
	}

	public void runQuery(SparkQuery sq, int attr) {
		int range;
		long start;
		long end;
		Predicate p1;
		Predicate p2;
		long result;
		Calendar c = new GregorianCalendar();;
		switch (attr) {
		case 4:
			range = 50;
			int startNum = (int) (Math.random() * range * (1 - selectivity)) + 1;
			int endNum = Math.max(startNum + 1, startNum + (int) (range * selectivity));
			System.out.println("INFO: Running QUANTITY Query from " + startNum + " to " + endNum);
			p1 = new Predicate(4, TYPE.INT, startNum, PREDTYPE.GT);
			p2 = new Predicate(4, TYPE.INT, endNum, PREDTYPE.LEQ);
			start = System.currentTimeMillis();
			result = sq.createAdaptRDD(cfg.getHDFS_WORKING_DIR(), p1, p2).count();
			end = System.currentTimeMillis();
			System.out.println("RES: QUANTITY " + (end - start) + " " + result);
			break;
		case 5:
			// -> range of EXTENDEDPRICE is 900 to 105000
			range = 50000;
			double startPrice = (Math.random() * range * (1 - selectivity)) + 25900;
			double endPrice = Math.max(startPrice + 1, startPrice + range * selectivity);
			System.out.println("INFO: Running EXTENDEDPRICE Query from " + startPrice + " to " + endPrice);
			p1 = new Predicate(5, TYPE.FLOAT, startPrice, PREDTYPE.GT);
			p2 = new Predicate(5, TYPE.FLOAT, endPrice, PREDTYPE.LEQ);
			start = System.currentTimeMillis();
			result = sq.createAdaptRDD(cfg.getHDFS_WORKING_DIR(), p1, p2).count();
			end = System.currentTimeMillis();
			System.out.println("RES: EXTENDEDPRICE " + (end - start) + " " + result);
			break;
		case 6:
			range = 10;
			double startDisc = (int) (Math.random() * range * (1 - selectivity)) * 0.01;
			double endDisc = Math.max(startDisc + 0.01, startDisc + (int) (range * selectivity) * 0.01);
			System.out.println("INFO: Running DISCOUNT Query from " + startDisc + " to " + endDisc);
			p1 = new Predicate(6, TYPE.FLOAT, startDisc, PREDTYPE.GT);
			p2 = new Predicate(6, TYPE.FLOAT, endDisc, PREDTYPE.LEQ);
			start = System.currentTimeMillis();
			result = sq.createAdaptRDD(cfg.getHDFS_WORKING_DIR(), p1, p2).count();
			end = System.currentTimeMillis();
			System.out.println("RES: DISCOUNT " + (end - start) + " " + result);
			break;
		case 10:
			range = 2525;
			int startOffset = (int) (Math.random() * range * (1 - selectivity)) + 1;
			c.set(1992, Calendar.JANUARY, 02);
			c.add(Calendar.DAY_OF_MONTH, startOffset);
			SimpleDate startDate = new SimpleDate(c.get(Calendar.YEAR), c.get(Calendar.MONTH) + 1, c.get(Calendar.DAY_OF_MONTH));
			c.add(Calendar.DAY_OF_MONTH, (int) (range * selectivity));
			SimpleDate endDate = new SimpleDate(c.get(Calendar.YEAR), c.get(Calendar.MONTH) + 1, c.get(Calendar.DAY_OF_MONTH));
			System.out.println("INFO: Running SHIPDATE Query from " + startDate.toString() + " to " + endDate.toString());

			start = System.currentTimeMillis();
			p1 = new Predicate(10, TYPE.DATE, startDate, PREDTYPE.GT);
			p2 = new Predicate(10, TYPE.DATE, endDate, PREDTYPE.LEQ);
			result = sq.createAdaptRDD(cfg.getHDFS_WORKING_DIR(), p1, p2).count();
			end = System.currentTimeMillis();
			System.out.println("RES: SHIPDATE " + (end - start) + " " + result);
			break;
		case 12:
			range = 2554;
			int startROffset = (int) (Math.random() * range * (1 - selectivity)) + 1;
			c.set(1992, Calendar.JANUARY, 03);
			c.add(Calendar.DAY_OF_MONTH, startROffset);
			SimpleDate startRDate = new SimpleDate(c.get(Calendar.YEAR), c.get(Calendar.MONTH) + 1, c.get(Calendar.DAY_OF_MONTH));
			c.add(Calendar.DAY_OF_MONTH, (int) (range * selectivity));
			SimpleDate endRDate = new SimpleDate(c.get(Calendar.YEAR), c.get(Calendar.MONTH) + 1, c.get(Calendar.DAY_OF_MONTH));
			System.out.println("INFO: Running RECEIPTDATE Query from " + startRDate.toString() + " to " + endRDate.toString());

			start = System.currentTimeMillis();
			p1 = new Predicate(12, TYPE.DATE, startRDate, PREDTYPE.GT);
			p2 = new Predicate(12, TYPE.DATE, endRDate, PREDTYPE.LEQ);
			result = sq.createAdaptRDD(cfg.getHDFS_WORKING_DIR(), p1, p2).count();
			end = System.currentTimeMillis();
			System.out.println("RES: RECEIPTDATE " + (end - start) + " " + result);
			break;
		case 14:
			String mode = shipModes[(int) (Math.random() * shipModes.length)];
			System.out.println("INFO: Running SHIP MODE Query "+mode);
			p1 = new Predicate(14, TYPE.STRING, mode, PREDTYPE.EQ);
			start = System.currentTimeMillis();
			result = sq.createAdaptRDD(cfg.getHDFS_WORKING_DIR(), p1).count();
			end = System.currentTimeMillis();
			System.out.println("RES: SHIP MODE " + (end - start) + " " + result);
			break;
		default:
			System.out.println("Unimplemented Attribute");
			break;
		}
	}

	/**
	 * Model:
	 * We have windows, in any window only two attributes are active
	 * At the start we have probability of attr1 = 1, other = 0 and
	 * at the end we have probability of attr1 = 0, other = 1
	 */
	public void testChangingQueries(){
		int[] attrs = new int[]{6,10,12};
		int numQueries = 30;
		SparkQuery sq = new SparkQuery(cfg);
		for (int i=0; i<attrs.length-1; i++) {
			int attr1 = attrs[i];
			int attr2 = attrs[i+1];
			float prob1 = (float) 1.0;
			for (int q=0; q<numQueries; q++) {
				Random r = new Random();
				if (r.nextFloat() <= prob1) {
					runQuery(sq, attr1);
				} else {
					runQuery(sq, attr2);
				}

				prob1 -= 1.0/numQueries;
			}
		}
	}

	// have two sets of attributes, switch between random queries on each set
	public void testSwitchingSets(){
		int[] attrsA = new int[]{4,5,6};
		int[] attrsB = new int[]{10,12,14};
		int numQueriesPerSet = 20;
		int numCycles = 3;
		SparkQuery sq = new SparkQuery(cfg);
		for (int i = 0; i < numCycles; i++) {
			for (int j = 0; j < numQueriesPerSet; j++) {
				int attr = attrsA[(int) (Math.random() * attrsA.length)];
				runQuery(sq, attr);
			}
			for (int j = 0; j < numQueriesPerSet; j++) {
				int attr = attrsB[(int) (Math.random() * attrsB.length)];
				runQuery(sq, attr);
			}
		}
	}

	public static void main(String[] args) {
		// need to reset the index and removePartitions.sh
		TestChangingWorkload tcw = new TestChangingWorkload();
		tcw.setUp();
		tcw.testChangingQueries();
		//tcw.testSwitchingSets();		
	}
}
