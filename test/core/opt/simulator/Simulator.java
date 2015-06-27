package core.opt.simulator;

import core.utils.CuratorUtils;
import junit.framework.TestCase;

import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.mapreduce.Job;

import core.access.Predicate;
import core.access.Predicate.PREDTYPE;
import core.access.Query.FilterQuery;
import core.adapt.opt.Optimizer;
import core.index.Settings;
import core.utils.ConfUtils;
import core.utils.HDFSUtils;
import core.utils.TypeUtils.*;

public class Simulator extends TestCase{
	String hdfsPath;
	Job job;
	Optimizer opt;
	int sf;
	final int TUPLES_PER_SF = 6000000;

	@Override
	public void setUp(){
		sf = 1000;
		ConfUtils cfg = new ConfUtils(Settings.cartilageConf);
		String hdfsHomeDir = Settings.hdfsPartitionDir;
		hdfsPath = cfg.getHADOOP_NAMENODE() + Settings.hdfsPartitionDir;
		
		// Cleanup queries file - to remove past query workload
		HDFSUtils.deleteFile(HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME()),
							hdfsHomeDir + "queries", false);

		CuratorFramework client = CuratorUtils.createAndStartClient(cfg.getZOOKEEPER_HOSTS());
		CuratorUtils.deleteAll(client, "/", "partition-");
		CuratorUtils.stopClient(client);

		opt = new Optimizer(hdfsPath, cfg.getHADOOP_HOME());
		opt.loadIndex(cfg.getZOOKEEPER_HOSTS());
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
		opt.updateCountsBasedOnSample(sf * TUPLES_PER_SF);
		/*
		int numQueries = 28;
		for (int i=0; i < numQueries; i++) {
			int year = 1992 + i % 7;
			Predicate p1 = new Predicate(10, TYPE.DATE, new SimpleDate(year-1,12,31), PREDTYPE.GT);
			Predicate p2 = new Predicate(10, TYPE.DATE, new SimpleDate(year,12,31), PREDTYPE.LEQ);
			opt.updateCountsBasedOnSample(sf * TUPLES_PER_SF);
			System.out.println("Updated Bucket Counts");
			opt.buildPlan(new FilterQuery(new Predicate[]{p1, p2}));
			System.out.println("INFO: Completed Query " + i);
		}
		*/
	}

	public void testRangePredicateRun() {
		int numQueries = 50;
		for (int i=1; i <= numQueries; i++) {
			int year = 1993 + (i + 1) % 5;
			Predicate p1 = new Predicate(10, TYPE.DATE, new SimpleDate(year-1,12,31), PREDTYPE.GT);
			Predicate p2 = new Predicate(10, TYPE.DATE, new SimpleDate(year,12,31), PREDTYPE.LEQ);
			opt.updateCountsBasedOnSample(sf * TUPLES_PER_SF);
			System.out.println("INFO: Updated Bucket Counts");
			opt.buildPlan(new FilterQuery(new Predicate[]{p1, p2}));
			System.out.println("INFO: Completed Query " + i);
		}
	}
}
