package core.access.benchmark;

import junit.framework.TestCase;
import core.access.Predicate;
import core.access.Predicate.PREDTYPE;
import core.access.spark.SparkQuery;
import core.index.Settings;
import core.utils.ConfUtils;
import core.utils.RangeUtils.SimpleDateRange.SimpleDate;
import core.utils.SchemaUtils.TYPE;

public class SingleAttributeQueries extends TestCase{
	public final static String propertyFile = Settings.cartilageConf;
	public final static ConfUtils cfg = new ConfUtils(propertyFile);

	@Override
	public void setUp() {

	}

	public void testSinglePredicateQueries(){
		int numQueries = 50;
		SparkQuery sq = new SparkQuery(cfg);
		for (int i=6; i <= 6; i++) {
			int year = 1993 + (i + 1) % 5;
			System.out.println("MDINDEX: Running Query " + i);
			Predicate p1 = new Predicate(10, TYPE.DATE, new SimpleDate(year-1,12,31), PREDTYPE.GT);
			long c = sq.createRDD("/user/anil/dodo", p1).count();
			System.out.println("Count = "+c);
			numQueries--;
		}
	}
}
