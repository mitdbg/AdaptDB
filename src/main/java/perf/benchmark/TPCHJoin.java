package perf.benchmark;



import core.adapt.spark.RangePartitioner;
import core.adapt.spark.SparkQuery;
import core.common.globals.Schema;
import core.utils.*;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

import core.adapt.Predicate;
import core.adapt.Query;
import core.adapt.Predicate.PREDTYPE;
import core.adapt.spark.SparkJoinQuery;
import core.common.globals.Globals;
import core.utils.TypeUtils.SimpleDate;
import core.utils.TypeUtils.TYPE;

import core.adapt.iterator.IteratorRecord;
import org.apache.spark.api.java.JavaRDD;

/**
 * Created by ylu on 12/2/15.
 */


public class TPCHJoin {

    public ConfUtils cfg;

    public Schema schemaCustomer, schemaLineitem, schemaNation, schemaOrders, schemaPart, schemaPartsupp, schemaRegion, schemaSupplier;
    public String stringCustomer, stringLineitem, stringNation, stringOrders, stringPart, stringPartsupp, stringRegion, stringSupplier;
    int numFields;

    private static String[] mktSegmentVals = new
            String[]{"AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY"};
    private static String[] regionNameVals = new
            String[]{"AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"};
    private static String[] partTypeVals = new
            String[]{"BRASS", "COPPER", "NICKEL", "STEEL", "TIN"};
    private static String[] shipModeVals = new
            String[]{"AIR", "FOB", "MAIL", "RAIL", "REG AIR", "SHIP", "TRUCK"};


    int method;

    int memoryBudget;

    int numQueries;

    Random rand;

    public void setUp() {
        cfg = new ConfUtils(BenchmarkSettings.conf);
        rand = new Random();

        // Making things more deterministic.
        rand.setSeed(0);

        //Globals.load(cfg.getHDFS_WORKING_DIR() + "/" + dataset + "/info", HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME()));

        //assert Globals.schema != null;

        //System.out.println(Globals.schema);

        // delete query history
        // Cleanup queries file - to remove past query workload
        //HDFSUtils.deleteFile(HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME()), cfg.getHDFS_WORKING_DIR() + "/" + dataset + "/queries", false);
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
                    schemaLineitem = Schema.createSchema(stringLineitem);
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


    public void postProcessing(String path, String schema) {

        /* rename part-0000i to i and create an info file*/

        try {
            FileSystem fs = HDFSUtils.getFS(cfg.getHADOOP_HOME()
                    + "/etc/hadoop/core-site.xml");

            // delete _SUCCESS

            String dest = path + "/data";

            fs.delete(new Path(dest + "/_SUCCESS"), false);
            FileStatus[] fileStatus = fs.listStatus(new Path(dest));

            for (int i = 0; i < fileStatus.length; i++) {
                String oldPath = fileStatus[i].getPath().toString();
                String baseName = FilenameUtils.getBaseName(oldPath);
                String dir = oldPath.substring(0, oldPath.length() - baseName.length());
                String newPath = dir + Integer.parseInt(baseName.substring(baseName.indexOf('-') + 1));

                fs.rename(new Path(oldPath), new Path(newPath));
            }


            /*  write out a fake (TOTAL_NUM_TUPLES is wrong) info to make HDFSPartition Happy*/

            Globals.schema = Schema.createSchema(schema);

            Globals.save(path + "/info", cfg.getHDFS_REPLICATION_FACTOR(), fs);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*
        select
            count(*)
        from
            customer,
            orders,
            lineitem
        where
            c_custkey = o_custkey
            and l_orderkey = o_orderkey
            and c_mktsegment = '[SEGMENT]'
            and o_orderdate < date '[DATE]'
            and l_shipdate > date '[DATE]'

        (lineitem ⋈ orders) ⋈ customer
     */

    public void tpch3() {

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

        Query q_c = null;
        Query q_o = new Query(new Predicate[]{p2_3});
        Query q_l = new Query(new Predicate[]{p3_3});


        if (rand_3 > 0) {
            String c_mktsegment_prev = mktSegmentVals[rand_3 - 1];
            Predicate p4_3 = new Predicate(schemaCustomer.getAttributeId("c_mktsegment"), TYPE.STRING, c_mktsegment_prev, PREDTYPE.GT);
            q_c = new Query(new Predicate[]{p1_3, p4_3});
        } else {
            q_c = new Query(new Predicate[]{p1_3});
        }

        System.out.println("INFO: Query_cutomer:" + q_c.toString());
        System.out.println("INFO: Query_orders:" + q_o.toString());
        System.out.println("INFO: Query_lineitem:" + q_l.toString());


        long start;

        SparkJoinQuery sq = new SparkJoinQuery(cfg);

        start = System.currentTimeMillis();


        // lineitem joins with orders, then with customers

        String lineitem = "lineitem";
        String orders = "orders";
        String customer = "customer";
        String lineitem_join_orders = "lineitem_join_orders";

        String stringLineitem_join_Orders = stringLineitem + ", " + stringOrders;

        JavaPairRDD<LongWritable, Text> rdd = sq.createJoinScanRDD(lineitem, stringLineitem, q_l, 0, "NULL", orders, stringOrders, q_o, 0, "NULL", memoryBudget);

        String cutPoints = sq.getCutPoints(customer, 0); // long[] = {1, 2, 3};

        Partitioner partitioner = new RangePartitioner(cutPoints);

        JavaRDD<Text> rdd_lineitem_join_orders = rdd.partitionBy(partitioner).values();

        String dest = cfg.getHDFS_WORKING_DIR() + "/" + lineitem_join_orders;

        rdd_lineitem_join_orders.saveAsTextFile(dest + "/data");

        long result = rdd_lineitem_join_orders.count();

        System.out.println("RES: Time Taken: " + (System.currentTimeMillis() - start) + "; Result: " + result);

        postProcessing(dest, stringLineitem_join_Orders);

        rdd = sq.createJoinScanRDD(customer, stringCustomer, q_c, 0, "NULL", lineitem_join_orders, stringLineitem_join_Orders, new Query(new Predicate[0]), 17, cutPoints, memoryBudget);

        result = rdd.count();

        System.out.println("RES: Time Taken: " + (System.currentTimeMillis() - start) + "; Result: " + result);
    }

    /*
        select
	        count(*)
        from
            customer,
            orders,
            lineitem,
            supplier
        where
            c_custkey = o_custkey
            and l_orderkey = o_orderkey
            and l_suppkey = s_suppkey
            and c_region = '[REGION]'
            and s_region = '[REGION]'
            and o_orderdate >= date '[DATE]'
            and o_orderdate < date '[DATE]' + interval '1' year

        （(customer ⋈ orders) ⋈ lineitem) ⋈ supplier
     */

    // under development
    public void tpch5() {
        int rand_5 = rand.nextInt(regionNameVals.length);
        String r_name_5 = regionNameVals[rand_5];
        int year_5 = 1993 + rand.nextInt(5);
        SimpleDate d5_1 = new SimpleDate(year_5, 1, 1);
        SimpleDate d5_2 = new SimpleDate(year_5 + 1, 1, 1);
        Predicate p1_5 = new Predicate(schemaCustomer.getAttributeId("c_region"), TYPE.STRING, r_name_5, PREDTYPE.LEQ);
        Predicate p2_5 = new Predicate(schemaSupplier.getAttributeId("s_region"), TYPE.STRING, r_name_5, PREDTYPE.LEQ);
        Predicate p3_5 = new Predicate(schemaOrders.getAttributeId("o_orderdate"), TYPE.DATE, d5_1, PREDTYPE.GEQ);
        Predicate p4_5 = new Predicate(schemaOrders.getAttributeId("o_orderdate"), TYPE.DATE, d5_2, PREDTYPE.LT);


        Query q_s = null;
        Query q_c = null;
        Query q_o = new Query(new Predicate[]{p3_5, p4_5});

        if (rand_5 > 0) {
            String r_name_prev_5 = regionNameVals[rand_5 - 1];
            Predicate p5_5 = new Predicate(schemaCustomer.getAttributeId("c_region"), TYPE.STRING, r_name_prev_5, PREDTYPE.GT);
            Predicate p6_5 = new Predicate(schemaSupplier.getAttributeId("s_region"), TYPE.STRING, r_name_prev_5, PREDTYPE.GT);
            q_s = new Query(new Predicate[]{p2_5,p6_5});
            q_c = new Query(new Predicate[]{p1_5,p5_5});
        } else {
            q_s = new Query(new Predicate[]{p2_5});
            q_c = new Query(new Predicate[]{p1_5});
        }

        System.out.println("INFO: Query_cutomer:" + q_c.toString());
        System.out.println("INFO: Query_orders:" + q_o.toString());
        System.out.println("INFO: Query_supplier:" + q_s.toString());


        long start;

        SparkJoinQuery sq = new SparkJoinQuery(cfg);

        start = System.currentTimeMillis();


        // lineitem joins with orders, then with customers

        String supplier = "supplier";
        String orders = "orders";
        String customer = "customer";
        String customer_join_orders = "customer_join_orders";

        String stringCustomer_join_Orders = stringCustomer + ", " + stringOrders;

        JavaPairRDD<LongWritable, Text> rdd = sq.createJoinScanRDD(customer, stringCustomer, q_c, 0, "NULL", orders, stringOrders, q_o, 1, "NULL", memoryBudget);

        String cutPoints = sq.getCutPoints(supplier, 0); // long[] = {1, 2, 3};

        Partitioner partitioner = new RangePartitioner(cutPoints);

        JavaRDD<Text> rdd_customer_join_orders = rdd.partitionBy(partitioner).values();

        String dest = cfg.getHDFS_WORKING_DIR() + "/" + customer_join_orders;

        rdd_customer_join_orders.saveAsTextFile(dest + "/data");

        long result = rdd_customer_join_orders.count();

        System.out.println("RES: Time Taken: " + (System.currentTimeMillis() - start) + "; Result: " + result);

        postProcessing(dest, stringCustomer_join_Orders);

        rdd = sq.createJoinScanRDD(customer, stringSupplier, q_s, 3, "NULL", customer_join_orders, stringCustomer_join_Orders, new Query(new Predicate[0]), 3, cutPoints, memoryBudget);

        result = rdd.count();

        System.out.println("RES: Time Taken: " + (System.currentTimeMillis() - start) + "; Result: " + result);
    }

    /*
        select
            count(*)
        from
            lineitem
        where
            l_shipdate >= date '[DATE]'
            and l_shipdate < date '[DATE]' + interval '1' year
            and l_discount between [DISCOUNT] - 0.01 and [DISCOUNT] + 0.01
            and l_quantity < [QUANTITY];
     */

    public void tpch6() {
        int year_6 = 1993 + rand.nextInt(5);
        SimpleDate d6_1 = new SimpleDate(year_6, 1, 1);
        SimpleDate d6_2 = new SimpleDate(year_6 + 1, 1, 1);
        double discount = rand.nextDouble() * 0.07 + 0.02;
        double quantity = rand.nextInt(2) + 24.0;
        Predicate p1_6 = new Predicate(schemaLineitem.getAttributeId("l_shipdate"), TYPE.DATE, d6_1, PREDTYPE.GEQ);
        Predicate p2_6 = new Predicate(schemaLineitem.getAttributeId("l_shipdate"), TYPE.DATE, d6_2, PREDTYPE.LT);
        Predicate p3_6 = new Predicate(schemaLineitem.getAttributeId("l_discount"), TYPE.DOUBLE, discount - 0.01, PREDTYPE.GT);
        Predicate p4_6 = new Predicate(schemaLineitem.getAttributeId("l_discount"), TYPE.DOUBLE, discount + 0.01, PREDTYPE.LEQ);
        Predicate p5_6 = new Predicate(schemaLineitem.getAttributeId("l_quantity"), TYPE.DOUBLE, quantity, PREDTYPE.LEQ);
        Query q_l = new Query(new Predicate[]{p1_6, p2_6, p3_6, p4_6, p5_6});

        long start = System.currentTimeMillis();

        SparkQuery sq = new SparkQuery(cfg);

        JavaPairRDD<LongWritable, IteratorRecord> rdd = sq.createScanRDD(cfg.getHDFS_WORKING_DIR(), q_l.getPredicates());
        long result = rdd.count();
        long end = System.currentTimeMillis();
        System.out.println("RES: Time Taken: " + (end - start) + "; Result: " + result);

    }


    /*
        select
            count(*)
        from
            part,
            lineitem,
            orders,
            customer
        where
            p_partkey = l_partkey
            and l_orderkey = o_orderkey
            and o_custkey = c_custkey
            and c_region = '[REGION]'
            and o_orderdate between date '1995-01-01' and date '1996-12-31'
            and p_type = '[TYPE]'

        ((lineitem ⋈ orders) ⋈ customer) ⋈ part
    */
    // under development
    public void tpch8() {

    }

    /*
        select
            count(*)
        from
            orders,
            lineitem
        where
            l_orderkey = o_orderkey
            and c_custkey = o_custkey
            and o_orderdate >= date '[DATE]'
            and o_orderdate < date '[DATE]' + interval '3' month
            and l_returnflag = 'R'

        (lineitem ⋈ orders) ⋈ customer
     */

    // under development
    public void tpch10() {

    }

    /*
        select
            count(*)
        from
            orders,
            lineitem
        where
            o_orderkey = l_orderkey
            and l_shipmode in ('[SHIPMODE1]', '[SHIPMODE2]')
            and l_receiptdate >= date '[DATE]'
            and l_receiptdate < date '[DATE]' + interval '1' year

        lineitem ⋈ orders
     */

    // under development
    public void tpch12() {

    }

    /*
        select
            count(*)
        from
            lineitem,
            part
        where
            l_partkey = p_partkey
            and l_shipdate >= date '[DATE]'
            and l_shipdate < date '[DATE]' + interval '1' month;

        lineitem ⋈ part
     */

    // under development
    public void tpch14() {

    }

    /*
        select
            count(*)
        from
            lineitem,
            part
        where
            p_partkey = l_partkey
            and l_shipinstruct = ‘DELIVER IN PERSON’
            and p_brand = ‘[BRAND]’
            and p_container = ‘SM CASE’
            and l_quantity >= [QUANTITY]
            and l_quantity <= [QUANTITY] + 10
            and p_size between 1 and 5
            and l_shipmode <= ‘AIR REG’

        lineitem ⋈ part
     */

    // under development
    public void tpch19() {

    }

    public static void main(String[] args) {

        BenchmarkSettings.loadSettings(args);
        BenchmarkSettings.printSettings();

        TPCHJoin t = new TPCHJoin();
        t.loadSettings(args);
        t.setUp();
        //t.tpch3();
        t.tpch5();
    }
}
