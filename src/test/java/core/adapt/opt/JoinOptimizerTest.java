package core.adapt.opt;

import core.adapt.AccessMethod.PartitionSplit;
import core.adapt.JoinQuery;
import core.adapt.Predicate;
import core.common.globals.Globals;
import core.common.globals.Schema;
import core.common.globals.TableInfo;
import core.common.index.JoinRobustTree;
import core.common.key.RawIndexKey;
import core.utils.ConfUtils;
import core.utils.HDFSUtils;
import core.utils.TypeUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Random;

/**
 * Created by ylu on 1/21/16.
 */
public class JoinOptimizerTest {

    private static Random rand = new Random();

    private static String lineitem = "lineitem", orders = "orders", customer = "customer", supplier = "supplier", part = "part";

    private static String[] mktSegmentVals = new
            String[]{"AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY"};
    private static String[] regionNameVals = new
            String[]{"AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"};
    private static String[] partTypeVals = new
            String[]{"BRASS", "COPPER", "NICKEL", "STEEL", "TIN"};
    private static String[] shipModeVals = new
            String[]{"AIR", "FOB", "MAIL", "RAIL", "REG AIR", "SHIP", "TRUCK"};

    private static Schema schemaCustomer, schemaLineitem, schemaOrders, schemaPart, schemaSupplier;
    private static String stringCustomer, stringLineitem, stringOrders, stringPart, stringSupplier;

    private static void setup() {
        stringLineitem = "l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate date, l_commitdate date, l_receiptdate date, l_shipinstruct string, l_shipmode string";
        schemaLineitem = Schema.createSchema(stringLineitem);
        rand.setSeed(0);
    }

    private static JoinQuery tpch3() {

        int rand_3 = rand.nextInt(mktSegmentVals.length);
        String c_mktsegment = mktSegmentVals[rand_3];
        Calendar c = new GregorianCalendar();
        int dateOffset = (int) (rand.nextFloat() * (31 + 28 + 31));
        c.set(1995, Calendar.MARCH, 01);
        c.add(Calendar.DAY_OF_MONTH, dateOffset);
        TypeUtils.SimpleDate d3 = new TypeUtils.SimpleDate(c.get(Calendar.YEAR),
                c.get(Calendar.MONTH), c.get(Calendar.DAY_OF_MONTH));


        Predicate p3_3 = new Predicate(schemaLineitem.getAttributeId("l_shipdate"), TypeUtils.TYPE.DATE, d3, Predicate.PREDTYPE.GT);

        JoinQuery q_c = null;
        JoinQuery q_l = new JoinQuery(lineitem, schemaLineitem.getAttributeId("l_orderkey"), new Predicate[]{p3_3});
        return q_l;
    }

    public static JoinQuery tpch5() {
        int rand_5 = rand.nextInt(regionNameVals.length);
        String r_name_5 = regionNameVals[rand_5];
        int year_5 = 1993 + rand.nextInt(5);
        TypeUtils.SimpleDate d5_1 = new TypeUtils.SimpleDate(year_5, 1, 1);
        TypeUtils.SimpleDate d5_2 = new TypeUtils.SimpleDate(year_5 + 1, 1, 1);
        JoinQuery q_l = new JoinQuery(lineitem, schemaLineitem.getAttributeId("l_suppkey"), new Predicate[]{});

        return q_l;

    }


    private static JoinQuery tpch10() {

        String l_returnflag_10 = "R";
        String l_returnflag_prev_10 = "N";
        int year_10 = 1993;
        int monthOffset = rand.nextInt(24);
        TypeUtils.SimpleDate d10_1 = new TypeUtils.SimpleDate(year_10 + monthOffset / 12, monthOffset % 12 + 1, 1);
        monthOffset = monthOffset + 3;
        TypeUtils.SimpleDate d10_2 = new TypeUtils.SimpleDate(year_10 + monthOffset / 12, monthOffset % 12 + 1, 1);
        Predicate p1_10 = new Predicate(schemaLineitem.getAttributeId("l_returnflag"), TypeUtils.TYPE.STRING, l_returnflag_10, Predicate.PREDTYPE.LEQ);
        Predicate p4_10 = new Predicate(schemaLineitem.getAttributeId("l_returnflag"), TypeUtils.TYPE.STRING, l_returnflag_prev_10, Predicate.PREDTYPE.GT);

        JoinQuery q_l = new JoinQuery(lineitem, schemaLineitem.getAttributeId("l_orderkey"), new Predicate[]{p1_10, p4_10});
        return q_l;
    }

    private static JoinQuery tpch12() {
        int rand_12 = rand.nextInt(shipModeVals.length);
        String shipmode_12 = shipModeVals[rand_12];
        int year_12 = 1993 + rand.nextInt(5);
        TypeUtils.SimpleDate d12_1 = new TypeUtils.SimpleDate(year_12, 1, 1);
        TypeUtils.SimpleDate d12_2 = new TypeUtils.SimpleDate(year_12 + 1, 1, 1);
        Predicate p1_12 = new Predicate(schemaLineitem.getAttributeId("l_shipmode"), TypeUtils.TYPE.STRING, shipmode_12, Predicate.PREDTYPE.LEQ);
        Predicate p2_12 = new Predicate(schemaLineitem.getAttributeId("l_receiptdate"), TypeUtils.TYPE.DATE, d12_1, Predicate.PREDTYPE.GEQ);
        Predicate p3_12 = new Predicate(schemaLineitem.getAttributeId("l_receiptdate"), TypeUtils.TYPE.DATE, d12_2, Predicate.PREDTYPE.LT);

        JoinQuery q_l = null;

        if (rand_12 > 0) {
            String shipmode_prev_12 = shipModeVals[rand_12 - 1];
            Predicate p4_12 = new Predicate(schemaLineitem.getAttributeId("l_shipmode"), TypeUtils.TYPE.STRING, shipmode_prev_12, Predicate.PREDTYPE.GT);
            q_l = new JoinQuery(lineitem, schemaLineitem.getAttributeId("l_orderkey"), new Predicate[]{p1_12, p2_12, p3_12, p4_12});
        } else {
            q_l = new JoinQuery(lineitem, schemaLineitem.getAttributeId("l_orderkey"), new Predicate[]{p1_12, p2_12, p3_12});
        }

        System.out.println("INFO: Query_lineitem:" + q_l.toString());

        return q_l;

    }

    private static JoinQuery tpch14() {
        int year_14 = 1993;
        int monthOffset_14 = rand.nextInt(60);
        TypeUtils.SimpleDate d14_1 = new TypeUtils.SimpleDate(year_14 + monthOffset_14 / 12, monthOffset_14 % 12 + 1, 1);
        monthOffset_14 += 1;
        TypeUtils.SimpleDate d14_2 = new TypeUtils.SimpleDate(year_14 + monthOffset_14 / 12, monthOffset_14 % 12 + 1, 1);
        Predicate p1_14 = new Predicate(schemaLineitem.getAttributeId("l_shipdate"), TypeUtils.TYPE.DATE, d14_1, Predicate.PREDTYPE.GEQ);
        Predicate p2_14 = new Predicate(schemaLineitem.getAttributeId("l_shipdate"), TypeUtils.TYPE.DATE, d14_2, Predicate.PREDTYPE.LT);
        JoinQuery q_l = new JoinQuery(lineitem,  schemaLineitem.getAttributeId("l_partkey"), new Predicate[]{p1_14, p2_14});
        return q_l;
    }

    private static JoinQuery tpch19() {

        String brand_19 = "Brand#" + (rand.nextInt(5) + 1) + "" + (rand.nextInt(5) + 1);
        String shipInstruct_19 = "DELIVER IN PERSON";
        double quantity_19 = rand.nextInt(10) + 1;
        Predicate p1_19 = new Predicate(schemaLineitem.getAttributeId("l_shipinstruct"), TypeUtils.TYPE.STRING, shipInstruct_19, Predicate.PREDTYPE.EQ);
        Predicate p4_19 = new Predicate(schemaLineitem.getAttributeId("l_quantity"), TypeUtils.TYPE.DOUBLE, quantity_19, Predicate.PREDTYPE.GT);
        quantity_19 += 10;
        Predicate p5_19 = new Predicate(schemaLineitem.getAttributeId("l_quantity"), TypeUtils.TYPE.DOUBLE, quantity_19, Predicate.PREDTYPE.LEQ);
        Predicate p8_19 = new Predicate(schemaLineitem.getAttributeId("l_shipmode"), TypeUtils.TYPE.STRING, "AIR", Predicate.PREDTYPE.LEQ);

        JoinQuery q_l = new JoinQuery(lineitem, schemaLineitem.getAttributeId("l_partkey"), new Predicate[]{p1_19, p4_19, p5_19, p8_19});

        return q_l;
    }

    public static void main(String[] args) {
        setup();

        ConfUtils cfg = new ConfUtils("/Users/ylu/Documents/workspace/mdindex/conf/ylu.properties");
        FileSystem fs = HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME());
        Globals.loadTableInfo(lineitem, cfg.getHDFS_WORKING_DIR(), fs);
        TableInfo tableInfo = Globals.getTableInfo(lineitem);



        JoinRobustTree.randGenerator.setSeed(0);

/*
        for (int i = 0; i < 10; i++) {

            JoinQuery q = tpch3();
            System.out.printf("TPCH3 Query %d: %s\n", i, q);
            // Load table info.

            JoinOptimizer opt = new JoinOptimizer(cfg);
            opt.loadIndex(tableInfo);
            opt.loadQueries(tableInfo);
            PartitionSplit[] split = opt.buildPlan(q);

        }

        for (int i = 0; i < 10; i++) {

            JoinQuery q = tpch10();
            System.out.printf("TPCH10 Query %d: %s\n", i, q);
            // Load table info.

            JoinOptimizer opt = new JoinOptimizer(cfg);
            opt.loadIndex(tableInfo);
            opt.loadQueries(tableInfo);
            PartitionSplit[] split = opt.buildPlan(q);

        }

*/



/*

        for (int i = 0; i < 10; i++) {

            JoinQuery q = tpch12();
            System.out.printf("TPCH12 Query %d: %s\n", i, q);
            // Load table info.

            JoinOptimizer opt = new JoinOptimizer(cfg);
            opt.loadIndex(tableInfo);
            opt.loadQueries(tableInfo);
            PartitionSplit[] split = opt.buildPlan(q);

        }

*/
/*

        for (int i = 0; i < 5; i++) {

            JoinQuery q = tpch3();
            System.out.printf("TPCH3 Query %d: %s\n", i, q);
            // Load table info.

            //q.setForceRepartition(i == 0);
            JoinOptimizer opt = new JoinOptimizer(cfg);
            opt.loadIndex(tableInfo);
            //opt.test();
            //opt.loadQueries(tableInfo);
            opt.test();
            System.out.println("build a plan");
            opt.buildPlan(q);
            //opt.test();
        }
        System.out.println("---------------------");
        //JoinOptimizer opt = new JoinOptimizer(cfg);
        //opt.loadIndex(tableInfo);
        //opt.test();
*/



        for (int k = 0; k < 1; k++) {
            JoinQuery q = tpch5();
            System.out.printf("TPCH5 Query %d: %s\n", k, q);

            q.setForceRepartition(true);
            // Load table info.

            JoinOptimizer opt = new JoinOptimizer(cfg);
            opt.loadIndex(tableInfo);
            //opt.test();


            //System.out.println("check " + opt.assertNotEmpty(opt.getIndex().getRoot()));
            opt.loadQueries(tableInfo);
            //opt.printTree();

            //opt.printBucketID(opt.getIndex().getRoot());

            PartitionSplit[] splits = opt.buildPlan(q);

            for(int i = 0; i< splits.length; i ++){
                int[] bids = splits[i].getPartitions();
                for (int j = 0; j < bids.length; j ++){
                    System.out.println(bids[j] + " ");
                }
            }
            System.out.println();
        }

    }

}
