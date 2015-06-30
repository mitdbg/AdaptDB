package core.access.benchmark;

import java.util.Random;

import org.apache.hadoop.fs.FileSystem;

import core.access.Predicate;
import core.access.Predicate.PREDTYPE;
import core.access.Query.FilterQuery;
import core.access.spark.Config;
import core.adapt.opt.WorkloadAnalyser;
import core.index.Settings;
import core.index.robusttree.RobustTreeHs;
import core.utils.ConfUtils;
import core.utils.HDFSUtils;
import core.utils.SchemaUtils.TYPE;

public class RunWorkloadAnalyser {
	ConfUtils cfg;
	
	public RunWorkloadAnalyser(ConfUtils cfg) {
		this.cfg = cfg;
	}

	public void generateQueries() {
		int numQueries = 15;
	
		String toWrite = "";
		for (int i=1; i <= numQueries; i++) {
			// DISCOUNT - Random (0.02 - 0.09); DISCOUNT+-0.01 range
			Random r = new Random();
			double ddisc = r.nextFloat() * 0.07 + 0.02;
			float disc = (float) ddisc;
			Predicate p1 = new Predicate(6, TYPE.FLOAT, disc - 0.01, PREDTYPE.GT);
			Predicate p2 = new Predicate(6, TYPE.FLOAT, disc + 0.01, PREDTYPE.LEQ);
			FilterQuery q = new FilterQuery(new Predicate[]{p1, p2});
			toWrite += q.toString() + "\n";
		}

		String pathToQueries = cfg.getHDFS_HOMEDIR() + "/queries";
		FileSystem fs = HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME());
		byte[] fileBytes = toWrite.getBytes();
		HDFSUtils.writeFile(fs, pathToQueries, Config.replication, fileBytes, 0, fileBytes.length, false);
	}
	
	public static void main(String[] args) {
		String propertyFile = Settings.cartilageConf;
		ConfUtils cfg = new ConfUtils(propertyFile);
		RunWorkloadAnalyser rwa = new RunWorkloadAnalyser(cfg);
		rwa.generateQueries();
		
		WorkloadAnalyser wa = new WorkloadAnalyser(cfg, 4, 2);
		RobustTreeHs rt = wa.getOptTree();
		String index = new String(rt.marshall());
		
		System.out.println(index);
	}
}
