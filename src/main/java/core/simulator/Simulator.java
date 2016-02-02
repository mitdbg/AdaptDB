package core.simulator;


import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.mapreduce.Job;

import core.adapt.Predicate;
import core.adapt.Predicate.PREDTYPE;
import core.adapt.Query;
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

	public void setUp() {
		sf = 1000;
		cfg = new ConfUtils(BenchmarkSettings.conf);
		this.cleanUp();
		opt = new Optimizer(cfg);
		// TODO: Fix this.
		// opt.loadIndex();
	}

	public void cleanUp() {
		// Cleanup queries file - to remove past query workload
		HDFSUtils.deleteFile(HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME()),
				cfg.getHDFS_WORKING_DIR() + "/queries", false);

		CuratorFramework client = CuratorUtils.createAndStartClient(cfg
				.getZOOKEEPER_HOSTS());
		CuratorUtils.deleteAll(client, "/", "partition-");
		CuratorUtils.stopClient(client);
	}

	public void testRunQuery() {
		Predicate[] predicates = new Predicate[] { new Predicate(0, TYPE.LONG,
				3002147L, PREDTYPE.LEQ) };
		opt.buildPlan(new Query("lineitem", predicates));
	}

	public void testSinglePredicateRun() {
		int numQueries = 50;
		for (int i = 1; i <= numQueries; i++) {
			int year = 1993 + (i + 1) % 5;
			Predicate p1 = new Predicate(10, TYPE.DATE, new SimpleDate(
					year - 1, 12, 31), PREDTYPE.GT);
			// Predicate p2 = new Predicate(10, TYPE.DATE, new
			// SimpleDate(year+1,1,1), PREDTYPE.LT);
			System.out.println("Updated Bucket Counts");
			opt.buildPlan(new Query("lineitem", new Predicate[] { p1 }));
			System.out.println("Completed Query " + i);
		}
	}
}
