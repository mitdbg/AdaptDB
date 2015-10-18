package perf.benchmark;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import core.access.Predicate;
import core.access.Query;
import core.access.Predicate.PREDTYPE;
import core.access.Query.FilterQuery;
import core.access.spark.SparkQuery;
import core.index.robusttree.Globals;
import core.utils.ConfUtils;
import core.utils.HDFSUtils;
import core.utils.TypeUtils;
import core.utils.TypeUtils.TYPE;

public class CMTWorkload {
	public ConfUtils cfg;

	public String schemaString;

	int numFields;

	int method;

	public void setUp() {
		cfg = new ConfUtils(BenchmarkSettings.conf);

		Globals.load(cfg.getHDFS_WORKING_DIR() + "/info",
				HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME()));
		assert Globals.schema != null;

		// delete query history
		// Cleanup queries file - to remove past query workload
		HDFSUtils.deleteFile(HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME()),
				cfg.getHDFS_WORKING_DIR() + "/queries", false);
	}
	
	public Predicate getPredicate(String pred) {
		String[] parts = pred.split(" ");
		int attrId = Globals.schema.getAttributeId(parts[0].trim());
		
		if (attrId == -1) {
			throw new RuntimeException("Unknown attr: " + parts[0].trim());
		}
		
		TYPE attrType = Globals.schema.getType(attrId);
		Object value = TypeUtils.deserializeValue(attrType, parts[2].trim().replaceAll("'", ""));
		String predTypeStr = parts[1].trim();
		PREDTYPE predType;
		switch (predTypeStr) {
		case ">":
			predType = PREDTYPE.GT;
			break;
		case ">=":
			predType = PREDTYPE.GEQ;
			break;
		case "<":
			predType = PREDTYPE.LT;
			break;
		case "<=":
			predType = PREDTYPE.LEQ;
			break;
		case "=":
			predType = PREDTYPE.EQ;
			break;
		default:
			throw new RuntimeException("Unknown predType " + predTypeStr);
		}
		
		Predicate p = new Predicate(parts[0].trim(), attrType, value, predType);
		return p;
	}
	
	public List<FilterQuery> generateWorkload() {
		byte[] stringBytes = HDFSUtils.readFile(
				HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME()), 
				"/user/mdindex/cmt_queries.log");
		String queriesString = new String(stringBytes);
		String[] queries = queriesString.split("\n");
		List<FilterQuery> ret = new ArrayList<FilterQuery>();
		for (int i=0; i<queries.length; i++) {
			String query = queries[i];
			String[] predicates = query.split(";");
			List<Predicate> queryPreds = new ArrayList<Predicate>();
			for (int j=0; j<predicates.length; j++) {
				Predicate p = getPredicate(predicates[j]);
				queryPreds.add(p);
			}
			Predicate[] predArray = queryPreds.toArray(new Predicate[queryPreds.size()]);
			ret.add(new Query.FilterQuery(predArray));
		}
		
		return ret;
	}
	
	public void runWorkload() {
		long start, end;
		SparkQuery sq = new SparkQuery(cfg);
		List<FilterQuery> queries = generateWorkload();
		for (FilterQuery q: queries) {
			System.out.println("INFO: Query:" + q.toString());
		}
		
		for (FilterQuery q : queries) {
			start = System.currentTimeMillis();
			long result = sq.createAdaptRDD(cfg.getHDFS_WORKING_DIR(),
					q.getPredicates()).count();
			end = System.currentTimeMillis();
			System.out.println("RES: Time Taken: " + (end - start) + 
					"; Result: " + result);
		}
	}
	
	public void loadSettings(String[] args) {
		int counter = 0;
		while (counter < args.length) {
			switch (args[counter]) {
			case "--schema":
				schemaString = args[counter + 1];
				counter += 2;
				break;
			case "--numFields":
				numFields = Integer.parseInt(args[counter + 1]);
				counter += 2;
				break;
			case "--method":
				method = Integer.parseInt(args[counter + 1]);
				counter += 2;
				break;
			default:
				// Something we don't use
				counter += 2;
				break;
			}
		}
	}

	public static void main(String[] args) {
		BenchmarkSettings.loadSettings(args);
		BenchmarkSettings.printSettings();

		CMTWorkload t = new CMTWorkload();
		t.loadSettings(args);
		t.setUp();

		switch (t.method) {
		case 1:
			t.runWorkload();
			break;
		default:
			break;
		}
	}
}
