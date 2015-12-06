package perf.benchmark;


import core.adapt.AccessMethod;
import core.common.globals.Schema;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.spark.api.java.JavaPairRDD;

import java.util.Random;

import core.adapt.Predicate;
import core.adapt.Query;
import core.adapt.Predicate.PREDTYPE;
import core.adapt.spark.SparkJoinQuery;
import core.common.globals.Globals;
import core.utils.ConfUtils;
import core.utils.HDFSUtils;
import core.utils.TypeUtils.SimpleDate;
import core.utils.TypeUtils.TYPE;


import core.adapt.iterator.IteratorRecord;

/**
 * Created by ylu on 12/2/15.
 */


public class TPCHJoin {
    public ConfUtils cfg;

    public Schema schemaCustomer, schemaLineitem, schemaNation, schemaOrders, schemaPart, schemaPartsupp, schemaRegion, schemaSupplier;
    public String stringCustomer, stringLineitem, stringNation, stringOrders, stringPart, stringPartsupp, stringRegion, stringSupplier;
    int numFields;

    int method;

    int memoryBudget;

    int numQueries;

    Random rand;

    public void setUp(String dataset) {
        cfg = new ConfUtils(BenchmarkSettings.conf);
        rand = new Random();

        // Making things more deterministic.
        rand.setSeed(0);

        //Globals.load(cfg.getHDFS_WORKING_DIR() + "/" + dataset + "/info", HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME()));

        //assert Globals.schema != null;

       //System.out.println(Globals.schema);

        // delete query history
        // Cleanup queries file - to remove past query workload
        HDFSUtils.deleteFile(HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME()),
                cfg.getHDFS_WORKING_DIR() + "/" + dataset + "/queries", false);
    }


    public void loadSettings(String[] args) {
        int counter = 0;
        while (counter < args.length) {
            switch (args[counter]) {
                case "--schemaCustomer":
                    stringCustomer = args[counter + 1];
                    schemaCustomer = Schema.createSchema(stringCustomer);
                    System.out.println(stringCustomer);
                    counter += 2;
                    break;
                case "--schemaLineitem":
                    stringLineitem = args[counter + 1];
                    schemaLineitem  = Schema.createSchema(stringLineitem);
                    System.out.println(stringLineitem);
                    counter += 2;
                    break;
                case "--schemaNation":
                    stringNation = args[counter + 1];
                    schemaNation = Schema.createSchema(stringNation);
                    counter += 2;
                    break;
                case "--schemaOrders":
                    stringOrders = args[counter + 1];
                    schemaOrders = Schema.createSchema(stringOrders);
                    counter += 2;
                    break;
                case "--schemaPart":
                    stringPart = args[counter + 1];
                    schemaPart = Schema.createSchema(stringPart);
                    counter += 2;
                    break;
                case "--schemaPartsupp":
                    stringPartsupp = args[counter + 1];
                    schemaPartsupp = Schema.createSchema(stringPartsupp);
                    counter += 2;
                    break;
                case "--schemaRegion":
                    stringRegion = args[counter + 1];
                    schemaRegion = Schema.createSchema(stringRegion);
                    counter += 2;
                    break;
                case "--schemaSupplier":
                    stringSupplier = args[counter + 1];
                    schemaSupplier = Schema.createSchema(stringSupplier);
                    counter += 2;
                    break;
                case "--method":
                    method = Integer.parseInt(args[counter + 1]);
                    counter += 2;
                    break;
                case "--numQueries":
                    numQueries = Integer.parseInt(args[counter + 1]);
                    counter += 2;
                    break;
                case "--budget":
                    memoryBudget = Integer.parseInt(args[counter + 1]);
                    counter += 2;
                    break;
                default:
                    // Something we don't use

                    counter += 2;
                    break;
            }
        }
    }

    public void runWorkload() {

        String dataset1 = "lineitem";
        String dataset2 = "orders";

        setUp(dataset1);

        System.out.println("Memory Stats (F/T/M): "
                + Runtime.getRuntime().freeMemory() + " "
                + Runtime.getRuntime().totalMemory() + " "
                + Runtime.getRuntime().maxMemory());


        String l_returnflag_10 = "R";
        String l_returnflag_prev_10 = "N";
        int year_10 = 1993;
        int monthOffset = rand.nextInt(24);
        SimpleDate d10_1 = new SimpleDate(year_10 + monthOffset / 12, monthOffset % 12 + 1, 1);
        monthOffset = monthOffset + 3;
        SimpleDate d10_2 = new SimpleDate(year_10 + monthOffset / 12, monthOffset % 12 + 1, 1);
        Predicate p1_10 = new Predicate(schemaLineitem.getAttributeId("l_returnflag"), TYPE.STRING, l_returnflag_10, PREDTYPE.LEQ);
        Predicate p4_10 = new Predicate(schemaLineitem.getAttributeId("l_returnflag"), TYPE.STRING, l_returnflag_prev_10, PREDTYPE.GT);
        Predicate p2_10 = new Predicate(schemaOrders.getAttributeId("o_orderdate"), TYPE.DATE, d10_1, PREDTYPE.GEQ);
        Predicate p3_10 = new Predicate(schemaOrders.getAttributeId("o_orderdate"), TYPE.DATE, d10_2, PREDTYPE.LT);
        Query q_l = new Query(new Predicate[]{p1_10, p4_10});
        Query q_o = new Query(new Predicate[]{p2_10, p3_10});

        long start, end;

        SparkJoinQuery sq = new SparkJoinQuery(cfg);

        System.out.println("INFO: Query_lineitem:" + q_l.toString());
        System.out.println("INFO: Query_orders:" + q_o.toString());


        start = System.currentTimeMillis();


        JavaPairRDD<LongWritable, IteratorRecord> rdd = sq.createJoinScanRDD(dataset1, 0, 0, stringLineitem,  q_l, dataset2, 0, 0, stringOrders, q_o, memoryBudget);

        long result = rdd.count();
        //rdd.saveAsTextFile(cfg.getHDFS_WORKING_DIR() + "/" + dataset1 + "_filtered");
        end = System.currentTimeMillis();
        System.out.println("RES: Time Taken: " + (end - start) + "; Result: " + result);

    }

    public static void main(String[] args) {

        BenchmarkSettings.loadSettings(args);
        BenchmarkSettings.printSettings();

        TPCHJoin t = new TPCHJoin();
        t.loadSettings(args);


        t.runWorkload();
    }
}
