package core.simulator;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Random;

import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.mapreduce.Job;

import core.access.AccessMethod.PartitionSplit;
import core.access.Predicate;
import core.access.Predicate.PREDTYPE;
import core.access.Query.FilterQuery;
import core.access.iterator.RepartitionIterator;
import core.adapt.opt.Optimizer;
import core.utils.ConfUtils;
import core.utils.CuratorUtils;
import core.utils.HDFSUtils;
import core.utils.TypeUtils.SimpleDate;
import core.utils.TypeUtils.TYPE;
import perf.benchmark.BenchmarkSettings;

public class Simulator {
	Job job;
	Optimizer opt;
	int sf;
	final long TUPLES_PER_SF = 6000000;
	ConfUtils cfg;

	public void setUp(){
		sf = 1000;
		cfg = new ConfUtils(BenchmarkSettings.conf);
		this.cleanUp();
		opt = new Optimizer(cfg);
		opt.loadIndex();
	}
	
	public void cleanUp() {
		// Cleanup queries file - to remove past query workload
		HDFSUtils.deleteFile(HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME()),
							cfg.getHDFS_WORKING_DIR() + "/queries", false);

		CuratorFramework client = CuratorUtils.createAndStartClient(cfg.getZOOKEEPER_HOSTS());
		CuratorUtils.deleteAll(client, "/", "partition-");
		CuratorUtils.stopClient(client);		
	}

	public void testRunQuery(){
		Predicate[] predicates = new Predicate[]{new Predicate(0, TYPE.LONG, 3002147L, PREDTYPE.LEQ)};
		opt.buildPlan(new FilterQuery(predicates));
	}

	public void testSinglePredicateRun() {
		int numQueries = 50;
		for (int i=1; i <= numQueries; i++) {
			int year = 1993 + (i + 1) % 5;
			Predicate p1 = new Predicate(10, TYPE.DATE, new SimpleDate(year-1,12,31), PREDTYPE.GT);
			// Predicate p2 = new Predicate(10, TYPE.DATE, new SimpleDate(year+1,1,1), PREDTYPE.LT);
			opt.updateCountsBasedOnSample(sf * TUPLES_PER_SF);
			System.out.println("Updated Bucket Counts");
			opt.buildPlan(new FilterQuery(new Predicate[]{p1}));
			System.out.println("Completed Query " + i);
		}
	}

	public void testCyclicShipDatePredicate() {
		int numQueries = 25;
		boolean doUpdate = true;
		for (int i=0; i < numQueries; i++) {
			int year = 1993 + i % 6;
			Predicate p1 = new Predicate(10, TYPE.DATE, new SimpleDate(year-1,12,31), PREDTYPE.GT);
			Predicate p2 = new Predicate(10, TYPE.DATE, new SimpleDate(year,12,31), PREDTYPE.LEQ);
			if (doUpdate) {
				opt.updateCountsBasedOnSample(sf * TUPLES_PER_SF);
				System.out.println("Updated Bucket Counts");
				doUpdate = false;
			}

			PartitionSplit[] splits = opt.buildPlan(new FilterQuery(new Predicate[]{p1, p2}));
			for (PartitionSplit ps : splits) {
				if (ps.getIterator() instanceof RepartitionIterator) {
					doUpdate = true;
					break;
				}
			}
			System.out.println("INFO: Completed Query " + i);
		}
	}

	public void testShipDateAdhoc() {
		boolean doUpdate = true;
		int numQueries = 30;
		double selectivity = 0.05;
		int range = 2525;
		Random r = new Random(1);
		Calendar c = new GregorianCalendar();
		for (int i=0; i < numQueries; i++) {
			int startOffset = (int) (r.nextDouble() * range * (1 - selectivity)) + 1;
			c.set(1992, Calendar.JANUARY, 02);
			c.add(Calendar.DAY_OF_MONTH, startOffset);
			SimpleDate startDate = new SimpleDate(c.get(Calendar.YEAR), c.get(Calendar.MONTH) + 1, c.get(Calendar.DAY_OF_MONTH));
			c.add(Calendar.DAY_OF_MONTH, (int) (range * selectivity));
			SimpleDate endDate = new SimpleDate(c.get(Calendar.YEAR), c.get(Calendar.MONTH) + 1, c.get(Calendar.DAY_OF_MONTH));

			Predicate p1 = new Predicate(10, TYPE.DATE, startDate, PREDTYPE.GT);
			Predicate p2 = new Predicate(10, TYPE.DATE, endDate, PREDTYPE.LEQ);
			if (doUpdate) {
				opt.updateCountsBasedOnSample(sf * TUPLES_PER_SF);
				System.out.println("Updated Bucket Counts");
				doUpdate = false;
			}

			PartitionSplit[] splits = opt.buildPlan(new FilterQuery(new Predicate[]{p1, p2}));
			for (PartitionSplit ps : splits) {
				if (ps.getIterator() instanceof RepartitionIterator) {
					doUpdate = true;
					break;
				}
			}
			System.out.println("INFO: Completed Query " + i);
		}
	}

	public void testShipDateDrillDown() {
		int numQueries = 30;
		double selectivity = 0.1;
		int range = 2525;
		Calendar c = new GregorianCalendar();
		Random r = new Random(1);
		int startOffset = (int) (r.nextDouble() * range * (1 - selectivity)) + 1;
		c.set(1992, Calendar.JANUARY, 02);
		c.add(Calendar.DAY_OF_MONTH, startOffset);
		SimpleDate startDate = new SimpleDate(c.get(Calendar.YEAR), c.get(Calendar.MONTH) + 1, c.get(Calendar.DAY_OF_MONTH));
		c.add(Calendar.DAY_OF_MONTH, (int) (range * selectivity));

		int reduction = (int) (range * selectivity * -1);
		boolean doUpdate = true;
		for (int i=0; i < numQueries; i++) {
			SimpleDate endDate = new SimpleDate(c.get(Calendar.YEAR), c.get(Calendar.MONTH) + 1, c.get(Calendar.DAY_OF_MONTH));
			Predicate p1 = new Predicate(10, TYPE.DATE, startDate, PREDTYPE.GT);
			Predicate p2 = new Predicate(10, TYPE.DATE, endDate, PREDTYPE.LEQ);
			if (doUpdate) {
				opt.updateCountsBasedOnSample(sf * TUPLES_PER_SF);
				System.out.println("Updated Bucket Counts");
				doUpdate = false;
			}

			PartitionSplit[] splits = opt.buildPlan(new FilterQuery(new Predicate[]{p1, p2}));
			for (PartitionSplit ps : splits) {
				if (ps.getIterator() instanceof RepartitionIterator) {
					doUpdate = true;
					break;
				}
			}
			reduction /= 2;
			c.add(Calendar.DAY_OF_MONTH, reduction);
			System.out.println("INFO: Completed Query " + i);
		}
	}
}
