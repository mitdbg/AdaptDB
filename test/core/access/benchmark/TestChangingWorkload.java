package core.access.benchmark;

import junit.framework.TestCase;
import scala.util.Random;
import core.access.Predicate;
import core.access.Predicate.PREDTYPE;
import core.access.spark.SparkQuery;
import core.index.Settings;
import core.utils.ConfUtils;
import core.utils.RangeUtils.SimpleDateRange.SimpleDate;
import core.utils.SchemaUtils.TYPE;

public class TestChangingWorkload extends TestCase{
	public final static String propertyFile = Settings.cartilageConf;
	public final static ConfUtils cfg = new ConfUtils(propertyFile);

	@Override
	public void setUp() {

	}

	public void runQuery(SparkQuery sq, int attr) {
		long c;
		Random r = new Random();
		switch (attr) {
		case 6:
			double ddisc = r.nextFloat() * 0.07 + 0.02;
			float disc = (float) ddisc;
			Predicate p1 = new Predicate(6, TYPE.FLOAT, disc - 0.01, PREDTYPE.GT);
			Predicate p2 = new Predicate(6, TYPE.FLOAT, disc + 0.01, PREDTYPE.LEQ);
			c = sq.createRDD("/user/anil/dodo", p1, p2).count();
			System.out.println("Num Records: "+c);
			break;
		case 10:
			int year = 1993 + r.nextInt() % 5;;
			Predicate p3 = new Predicate(10, TYPE.DATE, new SimpleDate(year-1,12,31), PREDTYPE.GT);
			Predicate p4 = new Predicate(10, TYPE.DATE, new SimpleDate(year,12,31), PREDTYPE.LEQ);
			c = sq.createRDD("/user/anil/dodo", p3, p4).count();
			System.out.println("Num Records: "+c);
			break;
		case 12:

		default:
			System.out.println("Unimplemented Attribute");
			break;
		}
	}

	/**
	 * Model:
	 * We have windows, in any window only two attributes are active
	 * At the start we have probability of attr1 = 1, other = 0 and
	 * at the end we have probability of attr1 = 0, other = 1
	 */
	public void testChangingQueries(){
		int[] attrs = new int[]{6,10,12};
		int numQueries = 25;
		SparkQuery sq = new SparkQuery(cfg);
		for (int i=0; i<attrs.length-1; i++) {
			int attr1 = attrs[i];
			int attr2 = attrs[i+1];
			float prob1 = (float) 1.0;
			for (int q=0; q<numQueries; q++) {
				Random r = new Random();
				if (r.nextFloat() <= prob1) {
					runQuery(sq, attr1);
				} else {
					runQuery(sq, attr2);
				}

				prob1 -= 1.0/25.0;
			}
		}
	}
}
