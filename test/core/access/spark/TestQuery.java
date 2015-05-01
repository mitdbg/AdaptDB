package core.access.spark;

import junit.framework.TestCase;
import core.access.Predicate;
import core.access.Predicate.PREDTYPE;
import core.index.Settings;
import core.utils.ConfUtils;
import core.utils.SchemaUtils.TYPE;

public class TestQuery extends TestCase{
	public final static String propertyFile = Settings.cartilageConf;
	public final static ConfUtils cfg = new ConfUtils(propertyFile);

	@Override
	public void setUp() {

	}

	public void testExecuteQuery(){
		Predicate p = new Predicate(0, TYPE.INT, 3002147, PREDTYPE.LEQ);
		SparkQuery sq = new SparkQuery(new Predicate[]{p}, cfg);
		sq.createRDD("/user/anil/dodo").count();

	}
}
