package core.simulator;


import core.adapt.Predicate;
import core.adapt.Predicate.PREDTYPE;
import core.adapt.Query;
import core.adapt.opt.Optimizer;
import core.common.globals.Globals;
import core.utils.ConfUtils;
import core.utils.CuratorUtils;
import core.utils.HDFSUtils;
import core.utils.TypeUtils.SimpleDate;
import core.utils.TypeUtils.TYPE;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import perf.benchmark.BenchmarkSettings;

public class Simulator {
	Optimizer opt;

	ConfUtils cfg;

	String simName;

	String dataset;

	Query[] queries;

	public void setUp(ConfUtils cfg, String simName, String dataset, Query[] queries) {
		this.cfg = cfg;
		this.simName = simName;
		this.dataset = dataset;
		this.queries = queries;

		FileSystem fs = HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME());

		HDFSUtils.deleteFile(fs,
				cfg.getHDFS_WORKING_DIR() + "/" + simName, true);
		HDFSUtils.safeCreateDirectory(fs, cfg.getHDFS_WORKING_DIR() + "/" + simName);

		CuratorFramework client = CuratorUtils.createAndStartClient(
				cfg.getZOOKEEPER_HOSTS());
		CuratorUtils.deleteAll(client, "/", "partition-");
		CuratorUtils.stopClient(client);

		HDFSUtils.copyFile(fs, cfg.getHDFS_WORKING_DIR() + "/" + dataset + "/" + "index",
				cfg.getHDFS_WORKING_DIR() + "/" + simName + "/" + "index",
				cfg.getHDFS_REPLICATION_FACTOR());
		HDFSUtils.copyFile(fs, cfg.getHDFS_WORKING_DIR() + "/" + dataset + "/" + "sample",
				cfg.getHDFS_WORKING_DIR() + "/" + simName + "/" + "sample",
				cfg.getHDFS_REPLICATION_FACTOR());
		HDFSUtils.copyFile(fs, cfg.getHDFS_WORKING_DIR() + "/" + dataset + "/" + "info",
				cfg.getHDFS_WORKING_DIR() + "/" + simName + "/" + "info",
				cfg.getHDFS_REPLICATION_FACTOR());

		opt = new Optimizer(cfg);
		opt.loadIndex(Globals.getTableInfo(simName));
	}

	public void run() {
		for (int i=0; i<queries.length; i++) {
			Query q = queries[i];
			q.setTable(simName);
			opt.buildPlan(q);
		}
	}
}
