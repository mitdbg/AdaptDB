package perf.benchmark;

import core.access.Predicate;
import core.access.spark.SparkQuery;
import core.utils.ConfUtils;
import core.utils.TypeUtils.TYPE;

/**
 * Created by qui on 6/12/15.
 */
public class TestRepartitioning {

    public final static String propertyFile = BenchmarkSettings.cartilageConf;
    public final static ConfUtils cfg = new ConfUtils(propertyFile);

    public void setUp() {

    }

    public void testBasic(){
        SparkQuery sq = new SparkQuery(cfg);
        Predicate p1 = new Predicate(8, TYPE.STRING, "R", Predicate.PREDTYPE.EQ);
        sq.createRepartitionRDD(cfg.getHDFS_WORKING_DIR(), p1).count();
    }

    public static void main(String[] args) {
        TestRepartitioning tp = new TestRepartitioning();
        tp.setUp();
        tp.testBasic();
    }
}
