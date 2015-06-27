package core.access.benchmark;

import core.access.Predicate;
import core.access.spark.SparkQuery;
import core.index.Settings;
import core.utils.ConfUtils;
import core.utils.TypeUtils.*;
import junit.framework.TestCase;

/**
 * Created by qui on 6/12/15.
 */
public class TestRepartitioning extends TestCase {

    public final static String propertyFile = Settings.cartilageConf;
    public final static ConfUtils cfg = new ConfUtils(propertyFile);

    @Override
    public void setUp() {

    }

    public void testBasic(){
        SparkQuery sq = new SparkQuery(cfg);
        Predicate p1 = new Predicate(8, TYPE.STRING, "R", Predicate.PREDTYPE.EQ);
        sq.createRepartitionRDD(Settings.hdfsPartitionDir, p1).count();
    }

    public static void main(String[] args) {
        TestRepartitioning tp = new TestRepartitioning();
        tp.setUp();
        tp.testBasic();
    }
}
