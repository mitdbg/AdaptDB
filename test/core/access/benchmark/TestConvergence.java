package core.access.benchmark;

import java.util.Random;

import junit.framework.TestCase;
import core.access.Predicate;
import core.access.Predicate.PREDTYPE;
import core.access.spark.SparkQuery;
import core.index.Settings;
import core.utils.ConfUtils;
import core.utils.RangeUtils.SimpleDateRange.SimpleDate;
import core.utils.SchemaUtils.TYPE;

public class TestConvergence extends TestCase{
	public final static String propertyFile = Settings.cartilageConf;
	public final static ConfUtils cfg = new ConfUtils(propertyFile);

	@Override
	public void setUp() {

	}

	public void testConvergenceShipDate(){
		int numQueries = 50;
		SparkQuery sq = new SparkQuery(cfg);
		for (int i=1; i <= numQueries; i++) {
			int year = 1993 + (i + 1) % 5;
			System.out.println("MDINDEX: Running Query " + i);
			Predicate p1 = new Predicate(10, TYPE.DATE, new SimpleDate(year-1,12,31), PREDTYPE.GT);
			Predicate p2 = new Predicate(10, TYPE.DATE, new SimpleDate(year,12,31), PREDTYPE.LEQ);
			long c = sq.createRDD("/user/anil/dodo", p1, p2).count();
			System.out.println("Count = "+c);
			numQueries--;
		}
	}

	public void testConvergenceDiscount() {
		int numQueries = 50;
		SparkQuery sq = new SparkQuery(cfg);
		for (int i=1; i <= numQueries; i++) {
			System.out.println("MDINDEX: Running Query " + i);
			// DISCOUNT - Random (0.02 - 0.09); DISCOUNT+-0.01 range
			Random r = new Random();
			double ddisc = r.nextFloat() * 0.07 + 0.02;
			float disc = (float) ddisc;
			Predicate p1 = new Predicate(6, TYPE.FLOAT, disc - 0.01, PREDTYPE.GT);
			Predicate p2 = new Predicate(6, TYPE.FLOAT, disc + 0.01, PREDTYPE.LEQ);
			long c = sq.createRDD("/user/anil/dodo", p1, p2).count();
			System.out.println("Count = "+c);
			numQueries--;
		}
	}
}
