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

	public void GenerateQueries(){
		int numQueries = 50;
		while (numQueries > 0) {
			int year = 1993 + numQueries % 5;
			Predicate p1 = new Predicate(0, TYPE.DATE, new SimpleDate(year,1,1), PREDTYPE.GEQ);
			Predicate p2 = new Predicate(0, TYPE.DATE, new SimpleDate(year,1,1), PREDTYPE.LT);
			SparkQuery sq = new SparkQuery(new Predicate[]{p1,p2}, cfg);
			sq.createRDD("/home/anil/dodo").count();
			numQueries--;
		}
	}
}
