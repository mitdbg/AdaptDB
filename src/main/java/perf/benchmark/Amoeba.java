package perf.benchmark;

import core.adapt.JoinQuery;
import core.adapt.Predicate;
import core.common.globals.Globals;
import core.common.globals.Schema;
import core.common.globals.TableInfo;
import core.common.index.JRNode;
import core.common.index.JoinRobustTree;
import core.common.index.MDIndex;
import core.common.key.ParsedTupleList;
import core.utils.ConfUtils;
import core.utils.HDFSUtils;
import core.utils.TypeUtils;
import org.apache.hadoop.fs.FileSystem;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Random;


/**
 * Created by ylu on 9/22/16.
 */
public class Amoeba {


    public static String lineitem = "lineitem";
    public static String orders = "orders";
    public static String customer = "customer";
    public static String part = "part";
    public static String supplier = "supplier";

    public static int lineitemBuckets = 16384, ordersBuckets = 2048, customerBuckets = 512, partBuckets = 512, supplierBuckets = 32;

    public static ParsedTupleList lineitemSample, ordersSample, customerSample, partSample, supplierSample;

    private static String[] mktSegmentVals = new
            String[]{"AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY"};
    private static String[] regionNameVals = new
            String[]{"AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"};
    private static String[] partTypeVals = new
            String[]{"BRASS", "COPPER", "NICKEL", "STEEL", "TIN"};
    private static String[] shipModeVals = new
            String[]{"AIR", "FOB", "MAIL", "RAIL", "REG AIR", "SHIP", "TRUCK"};


    public static ParsedTupleList loadSample(String tableName) {

        ConfUtils cfg = new ConfUtils("/Users/ylu/Documents/workspace/AdaptDB/conf/local.properties");
        FileSystem fs = HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME());
        // Load table info.
        Globals.loadTableInfo(tableName, cfg.getHDFS_WORKING_DIR(), fs);
        TableInfo tableInfo = Globals.getTableInfo(tableName);

        String pathToSample = cfg.getHDFS_WORKING_DIR() + "/" + tableInfo.tableName + "/sample";

        byte[] sampleBytes = HDFSUtils.readFile(fs, pathToSample);

        // read all the sample files and put them into the sample key set
        ParsedTupleList sample = new ParsedTupleList(tableInfo.getTypeArray());
        sample.unmarshall(sampleBytes, tableInfo.delimiter);

        return sample;
    }

    public static void init() {
        lineitemSample = loadSample(lineitem);
        ordersSample = loadSample(orders);
        customerSample = loadSample(customer);
        partSample = loadSample(part);
        supplierSample = loadSample(supplier);
    }

    public static JoinRobustTree getAdaptDbIndexWithQ(String tableName, ParsedTupleList sample, JoinQuery q, int numBuckets) {

        MDIndex.Bucket.maxBucketId = 0;

        TableInfo tableInfo = Globals.getTableInfo(tableName);

        JoinRobustTree rt = new JoinRobustTree(tableInfo);
        rt.joinAttributeDepth = 0;
        rt.setMaxBuckets(numBuckets);
        rt.loadSample(sample);
        rt.initProbe(q);

        return rt;
    }

    public static JoinRobustTree LineitemOnTpch3() {
        Random rand = new Random();

        String stringLineitem = "l_orderkey long, l_partkey int, l_suppkey int, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate date, l_commitdate date, l_receiptdate date, l_shipinstruct string, l_shipmode string";
        Schema schemaLineitem = Schema.createSchema(stringLineitem);

        int rand_3 = rand.nextInt(mktSegmentVals.length);
        String c_mktsegment = mktSegmentVals[rand_3];
        Calendar c = new GregorianCalendar();
        int dateOffset = (int) (rand.nextFloat() * (31 + 28 + 31));
        c.set(1995, Calendar.MARCH, 01);
        c.add(Calendar.DAY_OF_MONTH, dateOffset);
        TypeUtils.SimpleDate d3 = new TypeUtils.SimpleDate(c.get(Calendar.YEAR),
                c.get(Calendar.MONTH), c.get(Calendar.DAY_OF_MONTH));


        Predicate p3_3 = new Predicate(schemaLineitem.getAttributeId("l_shipdate"), TypeUtils.TYPE.DATE, d3, Predicate.PREDTYPE.GT);

        JoinQuery q_l = new JoinQuery(lineitem, schemaLineitem.getAttributeId("l_orderkey"), new Predicate[]{p3_3});

        return getAdaptDbIndexWithQ(lineitem, lineitemSample, q_l, lineitemBuckets);
    }


    public static JoinRobustTree LineitemOnTpch10() {
        Random rand = new Random();

        String stringLineitem = "l_orderkey long, l_partkey int, l_suppkey int, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate date, l_commitdate date, l_receiptdate date, l_shipinstruct string, l_shipmode string";
        Schema schemaLineitem = Schema.createSchema(stringLineitem);

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

        return getAdaptDbIndexWithQ(lineitem, lineitemSample, q_l, lineitemBuckets);
    }

    public static JoinRobustTree LineitemOnTpch12() {

        Random rand = new Random();

        String stringLineitem = "l_orderkey long, l_partkey int, l_suppkey int, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate date, l_commitdate date, l_receiptdate date, l_shipinstruct string, l_shipmode string";
        Schema schemaLineitem = Schema.createSchema(stringLineitem);


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

        return getAdaptDbIndexWithQ(lineitem, lineitemSample, q_l, lineitemBuckets);
    }

    public static JoinRobustTree LineitemOnTpch14() {

        Random rand = new Random();

        String stringLineitem = "l_orderkey long, l_partkey int, l_suppkey int, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate date, l_commitdate date, l_receiptdate date, l_shipinstruct string, l_shipmode string";
        Schema schemaLineitem = Schema.createSchema(stringLineitem);


        int year_14 = 1993;
        int monthOffset_14 = rand.nextInt(60);
        TypeUtils.SimpleDate d14_1 = new TypeUtils.SimpleDate(year_14 + monthOffset_14 / 12, monthOffset_14 % 12 + 1, 1);
        monthOffset_14 += 1;
        TypeUtils.SimpleDate d14_2 = new TypeUtils.SimpleDate(year_14 + monthOffset_14 / 12, monthOffset_14 % 12 + 1, 1);
        Predicate p1_14 = new Predicate(schemaLineitem.getAttributeId("l_shipdate"), TypeUtils.TYPE.DATE, d14_1, Predicate.PREDTYPE.GEQ);
        Predicate p2_14 = new Predicate(schemaLineitem.getAttributeId("l_shipdate"), TypeUtils.TYPE.DATE, d14_2, Predicate.PREDTYPE.LT);

        JoinQuery q_l = new JoinQuery(lineitem, schemaLineitem.getAttributeId("l_partkey"), new Predicate[]{p1_14, p2_14});


        return getAdaptDbIndexWithQ(lineitem, lineitemSample, q_l, lineitemBuckets);
    }

    public static JoinRobustTree LineitemOnTpch19() {
        Random rand = new Random();

        String stringLineitem = "l_orderkey long, l_partkey int, l_suppkey int, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate date, l_commitdate date, l_receiptdate date, l_shipinstruct string, l_shipmode string";
        Schema schemaLineitem = Schema.createSchema(stringLineitem);

        String brand_19 = "Brand#" + (rand.nextInt(5) + 1) + "" + (rand.nextInt(5) + 1);
        String shipInstruct_19 = "DELIVER IN PERSON";
        double quantity_19 = rand.nextInt(10) + 1;
        Predicate p1_19 = new Predicate(schemaLineitem.getAttributeId("l_shipinstruct"), TypeUtils.TYPE.STRING, shipInstruct_19, Predicate.PREDTYPE.EQ);

        Predicate p4_19 = new Predicate(schemaLineitem.getAttributeId("l_quantity"), TypeUtils.TYPE.DOUBLE, quantity_19, Predicate.PREDTYPE.GT);
        quantity_19 += 10;
        Predicate p5_19 = new Predicate(schemaLineitem.getAttributeId("l_quantity"), TypeUtils.TYPE.DOUBLE, quantity_19, Predicate.PREDTYPE.LEQ);
        Predicate p8_19 = new Predicate(schemaLineitem.getAttributeId("l_shipmode"), TypeUtils.TYPE.STRING, "AIR", Predicate.PREDTYPE.LEQ);

        JoinQuery q_l = new JoinQuery(lineitem, schemaLineitem.getAttributeId("l_partkey"), new Predicate[]{p1_19, p4_19, p5_19, p8_19});

        return getAdaptDbIndexWithQ(lineitem, lineitemSample, q_l, lineitemBuckets);
    }

    public static JoinRobustTree Orders(){

        Random rand = new Random();
        String stringOrders = "o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate date, o_orderpriority string, o_clerk string, o_shippriority int";
        Schema schemaOrders= Schema.createSchema(stringOrders);

        int rand_3 = rand.nextInt(mktSegmentVals.length);
        String c_mktsegment = mktSegmentVals[rand_3];
        Calendar c = new GregorianCalendar();
        int dateOffset = (int) (rand.nextFloat() * (31 + 28 + 31));
        c.set(1995, Calendar.MARCH, 01);
        c.add(Calendar.DAY_OF_MONTH, dateOffset);
        TypeUtils.SimpleDate d3 = new TypeUtils.SimpleDate(c.get(Calendar.YEAR),
                c.get(Calendar.MONTH), c.get(Calendar.DAY_OF_MONTH));


        Predicate p2_3 = new Predicate(schemaOrders.getAttributeId("o_orderdate"), TypeUtils.TYPE.DATE, d3, Predicate.PREDTYPE.LT);

        JoinQuery q_o = new JoinQuery(orders, schemaOrders.getAttributeId("o_orderkey"), new Predicate[]{p2_3});


        return getAdaptDbIndexWithQ(orders, ordersSample, q_o, ordersBuckets);
    }

    public static JoinRobustTree Part(){
        Random rand = new Random();

        String stringPart = "p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double";
        Schema schemaPart = Schema.createSchema(stringPart);


        String brand_19 = "Brand#" + (rand.nextInt(5) + 1) + "" + (rand.nextInt(5) + 1);
        String shipInstruct_19 = "DELIVER IN PERSON";
        double quantity_19 = rand.nextInt(10) + 1;

        Predicate p2_19 = new Predicate(schemaPart.getAttributeId("p_brand"), TypeUtils.TYPE.STRING, brand_19, Predicate.PREDTYPE.EQ);
        Predicate p3_19 = new Predicate(schemaPart.getAttributeId("p_container"), TypeUtils.TYPE.STRING, "SM CASE", Predicate.PREDTYPE.EQ);

        quantity_19 += 10;

        Predicate p6_19 = new Predicate(schemaPart.getAttributeId("p_size"), TypeUtils.TYPE.INT, 1, Predicate.PREDTYPE.GEQ);
        Predicate p7_19 = new Predicate(schemaPart.getAttributeId("p_size"), TypeUtils.TYPE.INT, 5, Predicate.PREDTYPE.LEQ);



        JoinQuery q_p = new JoinQuery(part, schemaPart.getAttributeId("p_partkey"), new Predicate[]{p2_19, p3_19, p6_19, p7_19});

        return getAdaptDbIndexWithQ(part, partSample, q_p, partBuckets);
    }


    public static JoinRobustTree Supplier(){

        Random rand = new Random();

        String stringSupplier = "s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string";
        Schema schemaSupplier = Schema.createSchema(stringSupplier);


        int rand_5 = rand.nextInt(regionNameVals.length);
        String r_name_5 = regionNameVals[rand_5];
        int year_5 = 1993 + rand.nextInt(5);
        TypeUtils.SimpleDate d5_1 = new TypeUtils.SimpleDate(year_5, 1, 1);
        TypeUtils.SimpleDate d5_2 = new TypeUtils.SimpleDate(year_5 + 1, 1, 1);

        Predicate p2_5 = new Predicate(schemaSupplier.getAttributeId("s_region"), TypeUtils.TYPE.STRING, r_name_5, Predicate.PREDTYPE.LEQ);


        JoinQuery q_s = null;

        if (rand_5 > 0) {
            String r_name_prev_5 = regionNameVals[rand_5 - 1];
            Predicate p6_5 = new Predicate(schemaSupplier.getAttributeId("s_region"), TypeUtils.TYPE.STRING, r_name_prev_5, Predicate.PREDTYPE.GT);
            q_s = new JoinQuery(supplier, schemaSupplier.getAttributeId("s_suppkey"), new Predicate[]{p2_5, p6_5});

        } else {
            q_s = new JoinQuery(supplier, schemaSupplier.getAttributeId("s_suppkey"), new Predicate[]{p2_5});
        }


        return getAdaptDbIndexWithQ(supplier, supplierSample, q_s, supplierBuckets);
    }

    public static JoinRobustTree CustomerOnTpch3(){
        Random rand = new Random();
        String stringCustomer = "c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string";
        Schema schemaCustomer = Schema.createSchema(stringCustomer);


        int rand_3 = rand.nextInt(mktSegmentVals.length);
        String c_mktsegment = mktSegmentVals[rand_3];
        Calendar c = new GregorianCalendar();
        int dateOffset = (int) (rand.nextFloat() * (31 + 28 + 31));
        c.set(1995, Calendar.MARCH, 01);
        c.add(Calendar.DAY_OF_MONTH, dateOffset);
        TypeUtils.SimpleDate d3 = new TypeUtils.SimpleDate(c.get(Calendar.YEAR),
                c.get(Calendar.MONTH), c.get(Calendar.DAY_OF_MONTH));


        Predicate p1_3 = new Predicate(schemaCustomer.getAttributeId("c_mktsegment"), TypeUtils.TYPE.STRING, c_mktsegment, Predicate.PREDTYPE.LEQ);


        JoinQuery q_c = null;


        if (rand_3 > 0) {
            String c_mktsegment_prev = mktSegmentVals[rand_3 - 1];
            Predicate p4_3 = new Predicate(schemaCustomer.getAttributeId("c_mktsegment"), TypeUtils.TYPE.STRING, c_mktsegment_prev, Predicate.PREDTYPE.GT);
            q_c = new JoinQuery(customer, schemaCustomer.getAttributeId("c_custkey"), new Predicate[]{p1_3, p4_3});
        } else {
            q_c = new JoinQuery(customer, schemaCustomer.getAttributeId("c_custkey"), new Predicate[]{p1_3});
        }

        return getAdaptDbIndexWithQ(customer, customerSample, q_c, customerBuckets);
    }

    public static JoinRobustTree CustomerOnTpch8(){
        Random rand = new Random();
        String stringCustomer = "c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string";
        Schema schemaCustomer = Schema.createSchema(stringCustomer);

        int rand_8_1 = rand.nextInt(regionNameVals.length);
        String r_name_8 = regionNameVals[rand_8_1];
        TypeUtils.SimpleDate d8_1 = new TypeUtils.SimpleDate(1995, 1, 1);
        TypeUtils.SimpleDate d8_2 = new TypeUtils.SimpleDate(1996, 12, 31);
        String p_type_8 = partTypeVals[rand.nextInt(partTypeVals.length)];
        Predicate p1_8 = new Predicate(schemaCustomer.getAttributeId("c_region"), TypeUtils.TYPE.STRING, r_name_8, Predicate.PREDTYPE.LEQ);

        JoinQuery q_c = null;


        if (rand_8_1 > 0) {
            String r_name_prev_8 = regionNameVals[rand_8_1 - 1];
            Predicate p5_8 = new Predicate(schemaCustomer.getAttributeId("c_region"), TypeUtils.TYPE.STRING, r_name_prev_8, Predicate.PREDTYPE.GT);
            q_c = new JoinQuery(customer, schemaCustomer.getAttributeId("c_custkey"), new Predicate[]{p1_8, p5_8});
        } else {
            q_c = new JoinQuery(customer, schemaCustomer.getAttributeId("c_custkey"), new Predicate[]{p1_8});
        }

        return getAdaptDbIndexWithQ(customer, customerSample, q_c, customerBuckets);
    }



    public static void main(String[] args) {

        init();


        System.out.println("Done!");
    }
}
