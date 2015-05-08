package core.opt.simulator;

import junit.framework.TestCase;

import org.apache.hadoop.mapreduce.Job;

import core.access.Predicate;
import core.access.Predicate.PREDTYPE;
import core.access.Query.FilterQuery;
import core.adapt.opt.Optimizer;
import core.index.Settings;
import core.utils.ConfUtils;
import core.utils.HDFSUtils;
import core.utils.RangeUtils.SimpleDateRange.SimpleDate;
import core.utils.SchemaUtils.TYPE;

public class Simulator extends TestCase{
	String hdfsPath;
	Job job;
	Optimizer opt;
	int sf;
	final int TUPLES_PER_SF = 6000000;

	@Override
	public void setUp(){
		sf = 1;
		hdfsPath = "hdfs://localhost:9000/user/anil/dodo";

		ConfUtils cfg = new ConfUtils(Settings.cartilageConf);

		// Cleanup queries file - to remove past query workload
		HDFSUtils.deleteFile(HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME()),
							"/user/anil/dodo/queries", false);

		opt = new Optimizer(hdfsPath, cfg.getHADOOP_HOME());
		opt.loadIndex(cfg.getZOOKEEPER_HOSTS());
	}

	public void testRunQuery(){
		Predicate[] predicates = new Predicate[]{new Predicate(0, TYPE.INT, 3002147, PREDTYPE.LEQ)};
		opt.buildPlan(new FilterQuery(predicates));
	}

	public void testSingleAttributeRun() {
		int numQueries = 50;
		for (int i=1; i <= numQueries; i++) {
			int year = 1993 + numQueries % 5;
			Predicate p1 = new Predicate(10, TYPE.DATE, new SimpleDate(year,1,1), PREDTYPE.GEQ);
			// Predicate p2 = new Predicate(10, TYPE.DATE, new SimpleDate(year+1,1,1), PREDTYPE.LT);
			opt.buildPlan(new FilterQuery(new Predicate[]{p1}));
			System.out.println("Completed Query " + i);
			opt.updateCountsBasedOnSample(sf * TUPLES_PER_SF);
			System.out.println("Updated Bucket Counts");
		}
	}
}
