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
        stringOrders = "o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate date, o_orderpriority string, o_clerk string, o_shippriority int";
        schemaOrders = Schema.createSchema(stringOrders);
        stringCustomer = "c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string";
        schemaCustomer = Schema.createSchema(stringCustomer);
        stringSupplier = "s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string";
        schemaSupplier = Schema.createSchema(stringSupplier);
        stringPart = "p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double";
        schemaPart = Schema.createSchema(stringPart);
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


        Predicate p1_3 = new Predicate(schemaCustomer.getAttributeId("c_mktsegment"), TypeUtils.TYPE.STRING, c_mktsegment, Predicate.PREDTYPE.LEQ);
        Predicate p2_3 = new Predicate(schemaOrders.getAttributeId("o_orderdate"), TypeUtils.TYPE.DATE, d3, Predicate.PREDTYPE.LT);
        Predicate p3_3 = new Predicate(schemaLineitem.getAttributeId("l_shipdate"), TypeUtils.TYPE.DATE, d3, Predicate.PREDTYPE.GT);

        JoinQuery q_c = null;
        JoinQuery q_o = new JoinQuery(orders, schemaOrders.getAttributeId("o_orderkey"), new Predicate[]{p2_3});
        JoinQuery q_l = new JoinQuery(lineitem, schemaLineitem.getAttributeId("l_orderkey"), new Predicate[]{p3_3});


        if (rand_3 > 0) {
            String c_mktsegment_prev = mktSegmentVals[rand_3 - 1];
            Predicate p4_3 = new Predicate(schemaCustomer.getAttributeId("c_mktsegment"), TypeUtils.TYPE.STRING, c_mktsegment_prev, Predicate.PREDTYPE.GT);
            q_c = new JoinQuery(customer, schemaCustomer.getAttributeId("c_custkey"), new Predicate[]{p1_3, p4_3});
        } else {
            q_c = new JoinQuery(customer, schemaCustomer.getAttributeId("c_custkey"), new Predicate[]{p1_3});
        }
        return q_l;
    }

    public static JoinQuery tpch5() {

        int rand_5 = rand.nextInt(regionNameVals.length);
        String r_name_5 = regionNameVals[rand_5];
        int year_5 = 1993 + rand.nextInt(5);
        TypeUtils.SimpleDate d5_1 = new TypeUtils.SimpleDate(year_5, 1, 1);
        TypeUtils.SimpleDate d5_2 = new TypeUtils.SimpleDate(year_5 + 1, 1, 1);
        Predicate p1_5 = new Predicate(schemaCustomer.getAttributeId("c_region"), TypeUtils.TYPE.STRING, r_name_5, Predicate.PREDTYPE.LEQ);
        Predicate p2_5 = new Predicate(schemaSupplier.getAttributeId("s_region"), TypeUtils.TYPE.STRING, r_name_5, Predicate.PREDTYPE.LEQ);
        Predicate p3_5 = new Predicate(schemaOrders.getAttributeId("o_orderdate"), TypeUtils.TYPE.DATE, d5_1, Predicate.PREDTYPE.GEQ);
        Predicate p4_5 = new Predicate(schemaOrders.getAttributeId("o_orderdate"), TypeUtils.TYPE.DATE, d5_2, Predicate.PREDTYPE.LT);


        JoinQuery q_s = null;
        JoinQuery q_c = null;
        JoinQuery q_o = new JoinQuery(orders, schemaOrders.getAttributeId("o_custkey"), new Predicate[]{p3_5, p4_5});
        JoinQuery q_l = new JoinQuery(lineitem, schemaLineitem.getAttributeId("l_suppkey"), new Predicate[]{});


        if (rand_5 > 0) {
            String r_name_prev_5 = regionNameVals[rand_5 - 1];
            Predicate p5_5 = new Predicate(schemaCustomer.getAttributeId("c_region"), TypeUtils.TYPE.STRING, r_name_prev_5, Predicate.PREDTYPE.GT);
            Predicate p6_5 = new Predicate(schemaSupplier.getAttributeId("s_region"), TypeUtils.TYPE.STRING, r_name_prev_5, Predicate.PREDTYPE.GT);
            q_s = new JoinQuery(supplier, schemaSupplier.getAttributeId("s_suppkey"), new Predicate[]{p2_5, p6_5});
            q_c = new JoinQuery(customer, schemaCustomer.getAttributeId("c_custkey"), new Predicate[]{p1_5, p5_5});
        } else {
            q_s = new JoinQuery(supplier, schemaSupplier.getAttributeId("s_suppkey"), new Predicate[]{p2_5});
            q_c = new JoinQuery(customer, schemaCustomer.getAttributeId("c_custkey"), new Predicate[]{p1_5});
        }
        return q_l;

    }


    private static JoinQuery tpch6(){
        int year_6 = 1993 + rand.nextInt(5);
        TypeUtils.SimpleDate d6_1 = new TypeUtils.SimpleDate(year_6, 1, 1);
        TypeUtils.SimpleDate d6_2 = new TypeUtils.SimpleDate(year_6 + 1, 1, 1);
        double discount = rand.nextDouble() * 0.07 + 0.02;
        double quantity = rand.nextInt(2) + 24.0;
        Predicate p1_6 = new Predicate(schemaLineitem.getAttributeId("l_shipdate"), TypeUtils.TYPE.DATE, d6_1, Predicate.PREDTYPE.GEQ);
        Predicate p2_6 = new Predicate(schemaLineitem.getAttributeId("l_shipdate"), TypeUtils.TYPE.DATE, d6_2, Predicate.PREDTYPE.LT);
        Predicate p3_6 = new Predicate(schemaLineitem.getAttributeId("l_discount"), TypeUtils.TYPE.DOUBLE, discount - 0.01, Predicate.PREDTYPE.GT);
        Predicate p4_6 = new Predicate(schemaLineitem.getAttributeId("l_discount"), TypeUtils.TYPE.DOUBLE, discount + 0.01, Predicate.PREDTYPE.LEQ);
        Predicate p5_6 = new Predicate(schemaLineitem.getAttributeId("l_quantity"), TypeUtils.TYPE.DOUBLE, quantity, Predicate.PREDTYPE.LEQ);
        JoinQuery q_l = new JoinQuery(lineitem, 0, new Predicate[]{p1_6, p2_6, p3_6, p4_6, p5_6});

        return q_l;

    }

    private static JoinQuery tpch8(){
        int rand_8_1 = rand.nextInt(regionNameVals.length);
        String r_name_8 = regionNameVals[rand_8_1];
        TypeUtils.SimpleDate d8_1 = new TypeUtils.SimpleDate(1995, 1, 1);
        TypeUtils.SimpleDate d8_2 = new TypeUtils.SimpleDate(1996, 12, 31);
        String p_type_8 = partTypeVals[rand.nextInt(partTypeVals.length)];
        Predicate p1_8 = new Predicate(schemaCustomer.getAttributeId("c_region"), TypeUtils.TYPE.STRING, r_name_8, Predicate.PREDTYPE.LEQ);
        Predicate p2_8 = new Predicate(schemaOrders.getAttributeId("o_orderdate"), TypeUtils.TYPE.DATE, d8_1, Predicate.PREDTYPE.GEQ);
        Predicate p3_8 = new Predicate(schemaOrders.getAttributeId("o_orderdate"), TypeUtils.TYPE.DATE, d8_2, Predicate.PREDTYPE.LEQ);
        Predicate p4_8 = new Predicate(schemaPart.getAttributeId("p_type"), TypeUtils.TYPE.STRING, p_type_8, Predicate.PREDTYPE.EQ);

        JoinQuery q_o = new JoinQuery(orders, schemaOrders.getAttributeId("o_custkey"), new Predicate[]{p2_8, p3_8});
        JoinQuery q_p = new JoinQuery(part, schemaPart.getAttributeId("p_partkey"), new Predicate[]{p4_8});
        JoinQuery q_c = null;
        JoinQuery q_l = new JoinQuery(lineitem, schemaLineitem.getAttributeId("l_partkey"), new Predicate[]{});

        return  q_l;
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




        for (int i = 0; i < 2; i++) {

            JoinQuery q = tpch3();
            System.out.printf("TPCH3 Query %d: %s\n", i, q);
            // Load table info.
            if(i==0){
                q.setForceRepartition(true);
            }

            JoinOptimizer opt = new JoinOptimizer(cfg);
            opt.loadIndex(tableInfo);

            opt.checkNotEmpty(opt.getIndex().getRoot());

            opt.loadQueries(tableInfo);
            opt.buildPlan(q);

        }


        for (int i = 0; i < 2; i++) {

            JoinQuery q = tpch5();
            System.out.printf("TPCH5 Query %d: %s\n", i, q);
            // Load table info.
            if(i==0){
                q.setForceRepartition(true);
            }

            JoinOptimizer opt = new JoinOptimizer(cfg);
            opt.loadIndex(tableInfo);

            opt.checkNotEmpty(opt.getIndex().getRoot());
            opt.loadQueries(tableInfo);
            opt.buildPlan(q);

        }

        for (int i = 0; i < 2; i++) {

            JoinQuery q = tpch6();
            System.out.printf("TPCH6 Query %d: %s\n", i, q);
            // Load table info.
            if(i==0){
                q.setForceRepartition(true);
            }

            JoinOptimizer opt = new JoinOptimizer(cfg);
            opt.loadIndex(tableInfo);

            opt.checkNotEmpty(opt.getIndex().getRoot());
            opt.loadQueries(tableInfo);
            opt.buildPlan(q);

        }

        for (int i = 0; i < 2; i++) {

            JoinQuery q = tpch8();
            System.out.printf("TPCH8 Query %d: %s\n", i, q);
            // Load table info.
            if(i==0){
                q.setForceRepartition(true);
            }

            JoinOptimizer opt = new JoinOptimizer(cfg);
            opt.loadIndex(tableInfo);

            opt.checkNotEmpty(opt.getIndex().getRoot());
            opt.loadQueries(tableInfo);
            opt.buildPlan(q);

        }


    }

}
