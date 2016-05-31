package perf.benchmark;

/**
 * Created by ylu on 4/25/16.
 */

import core.adapt.spark.RangePartitioner;
import core.adapt.spark.join.SparkJoinCopartitionedInputFormat;
import core.adapt.spark.join.SparkJoinQuery;
import core.adapt.spark.join.SparkJoinQueryConf;
import core.common.globals.Schema;
import core.common.globals.TableInfo;
import core.utils.ConfUtils;
import core.utils.HDFSUtils;
import core.utils.RangePartitionerUtils;
import core.utils.SparkUtils;
import core.utils.TypeUtils.SimpleDate;
import core.utils.TypeUtils.TYPE;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

import core.adapt.JoinQuery;
import core.adapt.Predicate;
import core.adapt.Predicate.PREDTYPE;
import core.utils.TypeUtils.SimpleDate;
import core.utils.TypeUtils.TYPE;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class TPCHCopartitionedWorkload {


    public static class Mapper implements PairFunction<String, Long, String> {
        String Delimiter;
        String Splitter;
        int partitionKey;
        public Mapper(String Delimiter, int partitionKey) {
            this.Delimiter = Delimiter;
            this.partitionKey = partitionKey;
            if (Delimiter.equals("|"))
                Splitter = "\\|";
            else
                Splitter = Delimiter;
        }

        @Override
        public Tuple2<Long, String> call(String v1) throws Exception {
            long key = Long.parseLong(v1.split(Splitter)[partitionKey]);
            return new Tuple2<Long, String>(key, v1);
        }
    }

    private ConfUtils cfg;

    private SparkJoinQuery sq;

    private Schema schemaCustomer, schemaLineitem, schemaOrders, schemaPart, schemaSupplier;
    private String stringCustomer, stringLineitem, stringOrders, stringPart, stringSupplier;
    private TableInfo tableLineitem, tableCustomer, tableOrders, tableSupplier, tablePart;

    private String lineitem = "lineitem", orders = "orders", customer = "customer", supplier = "supplier", part = "part";
    private Predicate[] EmptyPredicates = {};

    private static String[] mktSegmentVals = new
            String[]{"AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY"};
    private static String[] regionNameVals = new
            String[]{"AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"};
    private static String[] partTypeVals = new
            String[]{"BRASS", "COPPER", "NICKEL", "STEEL", "TIN"};
    private static String[] shipModeVals = new
            String[]{"AIR", "FOB", "MAIL", "RAIL", "REG AIR", "SHIP", "TRUCK"};




    private Random rand;
    private int method;

    public void setUp() {
        cfg = new ConfUtils(BenchmarkSettings.conf);
        sq = new SparkJoinQuery(cfg);
        rand = new Random();

        // Making things more deterministic.
        rand.setSeed(0);

        tableLineitem = new TableInfo(lineitem, 0, '|', schemaLineitem);
        tableCustomer = new TableInfo(customer, 0, '|', schemaCustomer);
        tableOrders = new TableInfo(orders, 0, '|', schemaOrders);
        tableSupplier = new TableInfo(supplier, 0, '|', schemaSupplier);
        tablePart = new TableInfo(part, 0, '|', schemaPart);


    }

    public void garbageCollect() {
        FileSystem fs = HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME());

        tableLineitem.gc(cfg.getHDFS_WORKING_DIR(), fs);
        tableOrders.gc(cfg.getHDFS_WORKING_DIR(), fs);

    }

    public void loadSettings(String[] args) {
        int counter = 0;
        while (counter < args.length) {
            switch (args[counter]) {
                case "--schemaCustomer":
                    stringCustomer = args[counter + 1];
                    schemaCustomer = Schema.createSchema(stringCustomer);
                    counter += 2;
                    break;
                case "--schemaLineitem":
                    stringLineitem = args[counter + 1];
                    schemaLineitem = Schema.createSchema(stringLineitem);
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
                case "--schemaSupplier":
                    stringSupplier = args[counter + 1];
                    schemaSupplier = Schema.createSchema(stringSupplier);
                    counter += 2;
                    break;
                case "--method":
                    method = Integer.parseInt(args[counter + 1]);
                    counter += 2;
                    break;
                default:
                    // Something we don't use
                    counter += 2;
                    break;
            }
        }
    }


    public void cleanup(String path) {
        FileSystem fs = HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME());
        try {

            fs.delete(new Path(path + "/_SUCCESS"), false);
            FileStatus[] fileStatus = fs.listStatus(new Path(path));

            for (int i = 0; i < fileStatus.length; i++) {
                String oldPath = fileStatus[i].getPath().toString();
                String baseName = FilenameUtils.getBaseName(oldPath);
                String dir = oldPath.substring(0, oldPath.length() - baseName.length());
                String newPath = dir + Integer.parseInt(baseName.substring(baseName.indexOf('-') + 1));
                fs.rename(new Path(oldPath), new Path(newPath));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void runCopartitioning(){
        JavaSparkContext ctx = sq.getSparkContext();

        HashPartitioner partitioner = new HashPartitioner(200);


        JavaRDD<String> lineitem_text = ctx.textFile(cfg.getHDFS_WORKING_DIR() + "/lineitem/data");
        JavaRDD<String> orders_text = ctx.textFile(cfg.getHDFS_WORKING_DIR() + "/orders/data");
        JavaRDD<String> customer_text = ctx.textFile(cfg.getHDFS_WORKING_DIR() + "/customer/data");
        JavaRDD<String> supplier_text = ctx.textFile(cfg.getHDFS_WORKING_DIR() + "/supplier/data");
        JavaRDD<String> part_text = ctx.textFile(cfg.getHDFS_WORKING_DIR() + "/part/data");


        JavaPairRDD<Long, String> lineitem_orderkey = lineitem_text.mapToPair(new Mapper("|", schemaLineitem.getAttributeId("l_orderkey")));
        JavaPairRDD<Long, String> lineitem_partkey = lineitem_text.mapToPair(new Mapper("|", schemaLineitem.getAttributeId("l_partkey")));
        JavaPairRDD<Long, String> lineitem_suppkey = lineitem_text.mapToPair(new Mapper("|", schemaLineitem.getAttributeId("l_suppkey")));


        JavaPairRDD<Long, String> orders_orderkey = orders_text.mapToPair(new Mapper("|", schemaOrders.getAttributeId("o_orderkey")));
        JavaPairRDD<Long, String> orders_custkey = orders_text.mapToPair(new Mapper("|", schemaOrders.getAttributeId("o_custkey")));

        JavaPairRDD<Long, String> customer_custkey = customer_text.mapToPair(new Mapper("|", schemaCustomer.getAttributeId("c_custkey")));
        JavaPairRDD<Long, String> supplier_suppkey = supplier_text.mapToPair(new Mapper("|", schemaSupplier.getAttributeId("s_suppkey")));
        JavaPairRDD<Long, String> part_partkey = part_text.mapToPair(new Mapper("|", schemaPart.getAttributeId("p_partkey")));


        JavaPairRDD<Long, String> lineitem_orderkey_partitioned = lineitem_orderkey.partitionBy(partitioner);
        JavaPairRDD<Long, String> lineitem_partkey_partitioned = lineitem_partkey.partitionBy(partitioner);
        JavaPairRDD<Long, String> lineitem_suppkey_partitioned = lineitem_suppkey.partitionBy(partitioner);


        JavaPairRDD<Long, String> orders_orderkey_partitioned = orders_orderkey.partitionBy(partitioner);
        JavaPairRDD<Long, String> orders_custkey_partitioned = orders_custkey.partitionBy(partitioner);


        JavaPairRDD<Long, String> customer_custkey_partitioned = customer_custkey.partitionBy(partitioner);
        JavaPairRDD<Long, String> supplier_suppkey_partitioned = supplier_suppkey.partitionBy(partitioner);
        JavaPairRDD<Long, String> part_partkey_partitioned = part_partkey.partitionBy(partitioner);

        lineitem_orderkey_partitioned.values().saveAsTextFile(cfg.getHDFS_WORKING_DIR() + "/lineitem_orderkey");
        lineitem_partkey_partitioned.values().saveAsTextFile(cfg.getHDFS_WORKING_DIR() + "/lineitem_partkey");
        lineitem_suppkey_partitioned.values().saveAsTextFile(cfg.getHDFS_WORKING_DIR() + "/lineitem_suppkey");

        orders_orderkey_partitioned.values().saveAsTextFile(cfg.getHDFS_WORKING_DIR() + "/orders_orderkey");
        orders_custkey_partitioned.values().saveAsTextFile(cfg.getHDFS_WORKING_DIR() + "/orders_custkey");


        customer_custkey_partitioned.values().saveAsTextFile(cfg.getHDFS_WORKING_DIR() + "/customer_custkey");
        supplier_suppkey_partitioned.values().saveAsTextFile(cfg.getHDFS_WORKING_DIR() + "/supplier_suppkey");
        part_partkey_partitioned.values().saveAsTextFile(cfg.getHDFS_WORKING_DIR() + "/part_partkey");


        cleanup(cfg.getHDFS_WORKING_DIR() + "/lineitem_orderkey");
        cleanup(cfg.getHDFS_WORKING_DIR() + "/lineitem_partkey");
        cleanup(cfg.getHDFS_WORKING_DIR() + "/lineitem_suppkey");

        cleanup(cfg.getHDFS_WORKING_DIR() + "/orders_orderkey");
        cleanup(cfg.getHDFS_WORKING_DIR() + "/orders_custkey");

        cleanup(cfg.getHDFS_WORKING_DIR() + "/customer_custkey");
        cleanup(cfg.getHDFS_WORKING_DIR() + "/supplier_suppkey");
        cleanup(cfg.getHDFS_WORKING_DIR() + "/part_partkey");


    }


    public void runCopartitionedTPCH3() {

        int rand_3 = rand.nextInt(mktSegmentVals.length);
        String c_mktsegment = mktSegmentVals[rand_3];
        Calendar c = new GregorianCalendar();
        int dateOffset = (int) (rand.nextFloat() * (31 + 28 + 31));
        c.set(1995, Calendar.MARCH, 01);
        c.add(Calendar.DAY_OF_MONTH, dateOffset);
        SimpleDate d3 = new SimpleDate(c.get(Calendar.YEAR),
                c.get(Calendar.MONTH), c.get(Calendar.DAY_OF_MONTH));


        Predicate p1_3 = new Predicate(schemaCustomer.getAttributeId("c_mktsegment"), TYPE.STRING, c_mktsegment, PREDTYPE.LEQ);
        Predicate p2_3 = new Predicate(schemaOrders.getAttributeId("o_orderdate"), TYPE.DATE, d3, PREDTYPE.LT);
        Predicate p3_3 = new Predicate(schemaLineitem.getAttributeId("l_shipdate"), TYPE.DATE, d3, PREDTYPE.GT);

        JoinQuery q_c = null;
        JoinQuery q_o = new JoinQuery(orders, schemaOrders.getAttributeId("o_orderkey"), new Predicate[]{p2_3});
        JoinQuery q_l = new JoinQuery(lineitem, schemaLineitem.getAttributeId("l_orderkey"), new Predicate[]{p3_3});


        if (rand_3 > 0) {
            String c_mktsegment_prev = mktSegmentVals[rand_3 - 1];
            Predicate p4_3 = new Predicate(schemaCustomer.getAttributeId("c_mktsegment"), TYPE.STRING, c_mktsegment_prev, PREDTYPE.GT);
            q_c = new JoinQuery(customer, schemaCustomer.getAttributeId("c_custkey"), new Predicate[]{p1_3, p4_3});
        } else {
            q_c = new JoinQuery(customer, schemaCustomer.getAttributeId("c_custkey"), new Predicate[]{p1_3});
        }


        String stringLineitem_join_Orders = stringLineitem + ", " + stringOrders;
        Schema schemaLineitem_join_Orders = Schema.createSchema(stringLineitem_join_Orders);

        JavaPairRDD<LongWritable, Text> lineitem_join_orders_rdd = sq.createCopartitionedRDD("lineitem_orderkey", q_l, "orders_orderkey", q_o, schemaLineitem_join_Orders.getAttributeId("o_custkey"));
        JavaPairRDD<LongWritable, Text> customer_rdd = sq.createScanRDD(customer, q_c);
        JavaPairRDD<LongWritable, Tuple2<Text, Text>> rdd = lineitem_join_orders_rdd.join(customer_rdd);


        long start = System.currentTimeMillis();

        long result = rdd.count();

        System.out.println("RES: Time Taken: " + (System.currentTimeMillis() - start) + "; Result: " + result);

    }

    public void runCopartitionedTPCH5(){

        int rand_5 = rand.nextInt(regionNameVals.length);
        String r_name_5 = regionNameVals[rand_5];
        int year_5 = 1993 + rand.nextInt(5);
        SimpleDate d5_1 = new SimpleDate(year_5, 1, 1);
        SimpleDate d5_2 = new SimpleDate(year_5 + 1, 1, 1);
        Predicate p1_5 = new Predicate(schemaCustomer.getAttributeId("c_region"), TYPE.STRING, r_name_5, PREDTYPE.LEQ);
        Predicate p2_5 = new Predicate(schemaSupplier.getAttributeId("s_region"), TYPE.STRING, r_name_5, PREDTYPE.LEQ);
        Predicate p3_5 = new Predicate(schemaOrders.getAttributeId("o_orderdate"), TYPE.DATE, d5_1, PREDTYPE.GEQ);
        Predicate p4_5 = new Predicate(schemaOrders.getAttributeId("o_orderdate"), TYPE.DATE, d5_2, PREDTYPE.LT);


        JoinQuery q_s = null;
        JoinQuery q_c = null;
        JoinQuery q_o = new JoinQuery(orders, schemaOrders.getAttributeId("o_custkey"), new Predicate[]{p3_5, p4_5});
        JoinQuery q_l = new JoinQuery(lineitem, schemaLineitem.getAttributeId("l_suppkey"), EmptyPredicates);


        if (rand_5 > 0) {
            String r_name_prev_5 = regionNameVals[rand_5 - 1];
            Predicate p5_5 = new Predicate(schemaCustomer.getAttributeId("c_region"), TYPE.STRING, r_name_prev_5, PREDTYPE.GT);
            Predicate p6_5 = new Predicate(schemaSupplier.getAttributeId("s_region"), TYPE.STRING, r_name_prev_5, PREDTYPE.GT);
            q_s = new JoinQuery(supplier, schemaSupplier.getAttributeId("s_suppkey"), new Predicate[]{p2_5, p6_5});
            q_c = new JoinQuery(customer, schemaCustomer.getAttributeId("c_custkey"), new Predicate[]{p1_5, p5_5});
        } else {
            q_s = new JoinQuery(supplier, schemaSupplier.getAttributeId("s_suppkey"), new Predicate[]{p2_5});
            q_c = new JoinQuery(customer, schemaCustomer.getAttributeId("c_custkey"), new Predicate[]{p1_5});
        }



        String stringCustomer_join_Orders = stringCustomer + ", " + stringOrders;
        Schema schemaCustomer_join_Orders = Schema.createSchema(stringCustomer_join_Orders);

        String stringLineitem_join_Supplier = stringLineitem + ", " + stringSupplier;
        Schema schemaLineitem_join_Supplier = Schema.createSchema(stringLineitem_join_Supplier);

        JavaPairRDD<LongWritable, Text> customer_join_orders_rdd =sq.createCopartitionedRDD("customer_custkey", q_c, "orders_custkey", q_o, schemaCustomer_join_Orders.getAttributeId("o_orderkey"));
        JavaPairRDD<LongWritable, Text> lineitem_join_supplier_rdd = sq.createCopartitionedRDD("lineitem_suppkey", q_l, "supplier_suppkey", q_s, schemaLineitem_join_Supplier.getAttributeId("l_orderkey"));


        JavaPairRDD<LongWritable, Tuple2<Text, Text>> rdd = customer_join_orders_rdd.join(lineitem_join_supplier_rdd);

        long start = System.currentTimeMillis();

        long result = rdd.count();

        System.out.println("RES: Time Taken: " + (System.currentTimeMillis() - start) + "; Result: " + result);

    }

    public void runCopartitionedTPCH8(){

        int rand_8_1 = rand.nextInt(regionNameVals.length);
        String r_name_8 = regionNameVals[rand_8_1];
        SimpleDate d8_1 = new SimpleDate(1995, 1, 1);
        SimpleDate d8_2 = new SimpleDate(1996, 12, 31);
        String p_type_8 = partTypeVals[rand.nextInt(partTypeVals.length)];
        Predicate p1_8 = new Predicate(schemaCustomer.getAttributeId("c_region"), TYPE.STRING, r_name_8, PREDTYPE.LEQ);
        Predicate p2_8 = new Predicate(schemaOrders.getAttributeId("o_orderdate"), TYPE.DATE, d8_1, PREDTYPE.GEQ);
        Predicate p3_8 = new Predicate(schemaOrders.getAttributeId("o_orderdate"), TYPE.DATE, d8_2, PREDTYPE.LEQ);
        Predicate p4_8 = new Predicate(schemaPart.getAttributeId("p_type"), TYPE.STRING, p_type_8, PREDTYPE.EQ);

        JoinQuery q_o = new JoinQuery(orders, schemaOrders.getAttributeId("o_custkey"), new Predicate[]{p2_8, p3_8});
        JoinQuery q_p = new JoinQuery(part, schemaPart.getAttributeId("p_partkey"), new Predicate[]{p4_8});
        JoinQuery q_c = null;
        JoinQuery q_l = new JoinQuery(lineitem, schemaLineitem.getAttributeId("l_partkey"), EmptyPredicates);

        if (rand_8_1 > 0) {
            String r_name_prev_8 = regionNameVals[rand_8_1 - 1];
            Predicate p5_8 = new Predicate(schemaCustomer.getAttributeId("c_region"), TYPE.STRING, r_name_prev_8, PREDTYPE.GT);
            q_c = new JoinQuery(customer, schemaCustomer.getAttributeId("c_custkey"), new Predicate[]{p1_8, p5_8});
        } else {
            q_c = new JoinQuery(customer, schemaCustomer.getAttributeId("c_custkey"), new Predicate[]{p1_8});
        }


        String stringOrders_join_Customer = stringOrders + ", " + stringCustomer;
        Schema schemaOrders_join_Customer = Schema.createSchema(stringOrders_join_Customer);
        String stringLineitem_join_Part = stringLineitem + ", " + stringPart;
        Schema schemaLineitem_join_Part = Schema.createSchema(stringLineitem_join_Part);


        JavaPairRDD<LongWritable, Text> orders_join_customer_rdd = sq.createCopartitionedRDD("orders_custkey", q_o, "customer_custkey", q_c, schemaOrders_join_Customer.getAttributeId("o_orderkey"));
        JavaPairRDD<LongWritable, Text> lineitem_join_part_rdd = sq.createCopartitionedRDD("lineitem_partkey", q_l,  "part_partkey", q_p, schemaLineitem_join_Part.getAttributeId("l_orderkey"));
        JavaPairRDD<LongWritable, Tuple2<Text, Text>> rdd = orders_join_customer_rdd.join(lineitem_join_part_rdd);

        long start = System.currentTimeMillis();

        long result = rdd.count();

        System.out.println("RES: Time Taken: " + (System.currentTimeMillis() - start) + "; Result: " + result);

    }

    public void runCopartitionedTPCH10(){

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

        JoinQuery q_l = new JoinQuery(lineitem, schemaLineitem.getAttributeId("l_orderkey"), new Predicate[]{p1_10, p4_10});
        JoinQuery q_o = new JoinQuery(orders, schemaOrders.getAttributeId("o_orderkey"), new Predicate[]{p2_10, p3_10});
        JoinQuery q_c = new JoinQuery(customer, schemaCustomer.getAttributeId("c_custkey"), EmptyPredicates);

        System.out.println("INFO: Query_lineitem:" + q_l.toString());
        System.out.println("INFO: Query_orders:" + q_o.toString());


        long start, end;


        String stringLineitem_join_Orders = stringLineitem + ", " + stringOrders;
        Schema schemaLineitem_join_Orders = Schema.createSchema(stringLineitem_join_Orders);

        start = System.currentTimeMillis();

        JavaPairRDD<LongWritable, Text> lineitem_join_orders_rdd = sq.createCopartitionedRDD("lineitem_orderkey", q_l,  "orders_orderkey", q_o, schemaLineitem_join_Orders.getAttributeId("o_custkey"));
        JavaPairRDD<LongWritable, Text> customer_rdd = sq.createScanRDD(customer, q_c);
        JavaPairRDD<LongWritable, Tuple2<Text, Text>> rdd = lineitem_join_orders_rdd.join(customer_rdd);

        long result = rdd.count();

        System.out.println("RES: Time Taken: " + (System.currentTimeMillis() - start) + "; Result: " + result);


    }

    public void runCopartitionedTPCH12(){
        int rand_12 = rand.nextInt(shipModeVals.length);
        String shipmode_12 = shipModeVals[rand_12];
        int year_12 = 1993 + rand.nextInt(5);
        SimpleDate d12_1 = new SimpleDate(year_12, 1, 1);
        SimpleDate d12_2 = new SimpleDate(year_12 + 1, 1, 1);
        Predicate p1_12 = new Predicate(schemaLineitem.getAttributeId("l_shipmode"), TYPE.STRING, shipmode_12, PREDTYPE.LEQ);
        Predicate p2_12 = new Predicate(schemaLineitem.getAttributeId("l_receiptdate"), TYPE.DATE, d12_1, PREDTYPE.GEQ);
        Predicate p3_12 = new Predicate(schemaLineitem.getAttributeId("l_receiptdate"), TYPE.DATE, d12_2, PREDTYPE.LT);

        JoinQuery q_l = null;
        JoinQuery q_o = new JoinQuery(orders, schemaOrders.getAttributeId("o_orderkey"), EmptyPredicates);

        if (rand_12 > 0) {
            String shipmode_prev_12 = shipModeVals[rand_12 - 1];
            Predicate p4_12 = new Predicate(schemaLineitem.getAttributeId("l_shipmode"), TYPE.STRING, shipmode_prev_12, PREDTYPE.GT);
            q_l = new JoinQuery(lineitem, schemaLineitem.getAttributeId("l_orderkey"), new Predicate[]{p1_12, p2_12, p3_12, p4_12});
        } else {
            q_l = new JoinQuery(lineitem, schemaLineitem.getAttributeId("l_orderkey"), new Predicate[]{p1_12, p2_12, p3_12});
        }

        System.out.println("INFO: Query_lineitem:" + q_l.toString());


        long start = System.currentTimeMillis();

        // lineitem ⋈ orders


        JavaPairRDD<LongWritable, Text> rdd = sq.createCopartitionedRDD("lineitem_orderkey", q_l, "orders_orderkey", q_o, 0);

        long result = rdd.count();
        System.out.println("RES: Time Taken: " + (System.currentTimeMillis() - start) + "; Result: " + result);

    }

    public void runCopartitionedTPCH14(){
        int year_14 = 1993;
        int monthOffset_14 = rand.nextInt(60);
        SimpleDate d14_1 = new SimpleDate(year_14 + monthOffset_14 / 12, monthOffset_14 % 12 + 1, 1);
        monthOffset_14 += 1;
        SimpleDate d14_2 = new SimpleDate(year_14 + monthOffset_14 / 12, monthOffset_14 % 12 + 1, 1);
        Predicate p1_14 = new Predicate(schemaLineitem.getAttributeId("l_shipdate"), TYPE.DATE, d14_1, PREDTYPE.GEQ);
        Predicate p2_14 = new Predicate(schemaLineitem.getAttributeId("l_shipdate"), TYPE.DATE, d14_2, PREDTYPE.LT);

        JoinQuery q_l = new JoinQuery(lineitem, schemaLineitem.getAttributeId("l_partkey"), new Predicate[]{p1_14, p2_14});
        JoinQuery q_p = new JoinQuery(part, schemaPart.getAttributeId("p_partkey"), EmptyPredicates);

        System.out.println("INFO: Query_lineitem:" + q_l.toString());



        long start = System.currentTimeMillis();

        // lineitem ⋈ part

        JavaPairRDD<LongWritable, Text> rdd = sq.createCopartitionedRDD("lineitem_partkey", q_l, "part_partkey", q_p, 0);

        long result = rdd.count();
        System.out.println("RES: Time Taken: " + (System.currentTimeMillis() - start) + "; Result: " + result);

    }

    public void runCopartitionedTPCH19(){
        String brand_19 = "Brand#" + (rand.nextInt(5) + 1) + "" + (rand.nextInt(5) + 1);
        String shipInstruct_19 = "DELIVER IN PERSON";
        double quantity_19 = rand.nextInt(10) + 1;
        Predicate p1_19 = new Predicate(schemaLineitem.getAttributeId("l_shipinstruct"), TYPE.STRING, shipInstruct_19, PREDTYPE.EQ);
        Predicate p2_19 = new Predicate(schemaPart.getAttributeId("p_brand"), TYPE.STRING, brand_19, PREDTYPE.EQ);
        Predicate p3_19 = new Predicate(schemaPart.getAttributeId("p_container"), TYPE.STRING, "SM CASE", PREDTYPE.EQ);
        Predicate p4_19 = new Predicate(schemaLineitem.getAttributeId("l_quantity"), TYPE.DOUBLE, quantity_19, PREDTYPE.GT);
        quantity_19 += 10;
        Predicate p5_19 = new Predicate(schemaLineitem.getAttributeId("l_quantity"), TYPE.DOUBLE, quantity_19, PREDTYPE.LEQ);
        Predicate p6_19 = new Predicate(schemaPart.getAttributeId("p_size"), TYPE.INT, 1, PREDTYPE.GEQ);
        Predicate p7_19 = new Predicate(schemaPart.getAttributeId("p_size"), TYPE.INT, 5, PREDTYPE.LEQ);
        Predicate p8_19 = new Predicate(schemaLineitem.getAttributeId("l_shipmode"), TYPE.STRING, "AIR", PREDTYPE.LEQ);

        JoinQuery q_l = new JoinQuery(lineitem, schemaLineitem.getAttributeId("l_partkey"), new Predicate[]{p1_19, p4_19, p5_19, p8_19});
        JoinQuery q_p = new JoinQuery(part, schemaPart.getAttributeId("p_partkey"), new Predicate[]{p2_19, p3_19, p6_19, p7_19});

        System.out.println("INFO: Query_lineitem:" + q_l.toString());
        System.out.println("INFO: Query_part:" + q_p.toString());


        long start = System.currentTimeMillis();

        // lineitem ⋈ part


        JavaPairRDD<LongWritable, Text> rdd = sq.createCopartitionedRDD("lineitem_partkey", q_l, "part_partkey", q_p, 0);

        long result = rdd.count();
        System.out.println("RES: Time Taken: " + (System.currentTimeMillis() - start) + "; Result: " + result);

    }

    public static void main(String[] args) {

        BenchmarkSettings.loadSettings(args);
        BenchmarkSettings.printSettings();

        TPCHCopartitionedWorkload t = new TPCHCopartitionedWorkload();
        t.loadSettings(args);
        t.setUp();


        switch (t.method) {
            case 1:
                t.runCopartitioning();
                break;
            case 2:
                t.runCopartitionedTPCH3();
                break;
            case 3:
                t.runCopartitionedTPCH5();
                break;
            case 4:
                t.runCopartitionedTPCH8();
                break;
            case 5:
                t.runCopartitionedTPCH10();
                break;
            case 6:
                t.runCopartitionedTPCH12();
                break;
            case 7:
                t.runCopartitionedTPCH14();
                break;
            case 8:
                t.runCopartitionedTPCH19();
                break;
            default:
                break;
        }

        //t.garbageCollect();
    }
}
