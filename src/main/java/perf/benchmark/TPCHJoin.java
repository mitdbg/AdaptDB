package perf.benchmark;


import core.adapt.AccessMethod;
import core.adapt.iterator.PartitionIterator;
import core.adapt.spark.RangePartitioner;
import core.common.globals.Schema;
import core.common.index.MDIndex;
import core.common.index.RobustTree;
import core.utils.*;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Options;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;

import java.io.DataInput;
import java.io.DataOutput;
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
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Created by ylu on 12/2/15.
 */


public class TPCHJoin {

    public ConfUtils cfg;

    public Schema schemaCustomer, schemaLineitem, schemaNation, schemaOrders, schemaPart, schemaPartsupp, schemaRegion, schemaSupplier;
    public String stringCustomer, stringLineitem, stringNation, stringOrders, stringPart, stringPartsupp, stringRegion, stringSupplier;
    int numFields;

    private static String[] mktSegmentVals = new
            String[]{"AUTOMOBILE","BUILDING","FURNITURE","HOUSEHOLD","MACHINERY"};
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


    /*
        select
            l_orderkey,
            sum(l_extendedprice*(1-l_discount)) as revenue, o_orderdate,
            o_shippriority
        from
            customer,
            orders,
            lineitem
        where
            c_mktsegment = '[SEGMENT]' and
            c_custkey = o_custkey and
            l_orderkey = o_orderkey and
            o_orderdate < date '[DATE]' and
            l_shipdate > date '[DATE]'
        group by l_orderkey,
            o_orderdate,
            o_shippriority
        order by
            revenue desc,
            o_orderdate;
     */


    public void tpch3(){

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
            q_c =  new Query(new Predicate[]{p1_3,p4_3});
        } else {
            q_c =  new Query(new Predicate[]{p1_3});
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


        JavaPairRDD<LongWritable, Text> rdd = sq.createJoinScanRDD(lineitem, stringLineitem,  q_l, 0, "NULL", orders, stringOrders, q_o, 0, "NULL", memoryBudget);


        String cutPoints =  sq.getCutPoints(customer, 0); // long[] = {1, 2, 3};

        Partitioner partitioner = new RangePartitioner(cutPoints);

        JavaRDD<Text> rdd_lineitem_join_orders = rdd.partitionBy(partitioner).values();

        String dest = cfg.getHDFS_WORKING_DIR() + "/" + lineitem_join_orders + "/data";

        rdd_lineitem_join_orders.saveAsTextFile(dest);

        /* rename part-0000i to i */

        long result = rdd_lineitem_join_orders.count();

        System.out.println("RES: Time Taken: " + (System.currentTimeMillis() - start) + "; Result: " + result);


        try{
            FileSystem fs = HDFSUtils.getFS(cfg.getHADOOP_HOME()
                    + "/etc/hadoop/core-site.xml");

            // delete _SUCCESS

            fs.delete(new Path(dest + "/_SUCCESS"), false);
            FileStatus[] fileStatus = fs.listStatus(new Path(dest));

            for(int i = 0; i < fileStatus.length; i ++) {
                String oldPath = fileStatus[i].getPath().toString();
                String baseName = FilenameUtils.getBaseName(oldPath);
                String dir = oldPath.substring(0, oldPath.length() - baseName.length());
                String newPath = dir + Integer.parseInt(baseName.substring(baseName.indexOf('-') + 1));

                fs.rename(new Path(oldPath), new Path(newPath));
            }


            /*  write out a fake (TOTAL_NUM_TUPLES is wrong) info to make HDFSPartition Happy*/

            Globals.schema = Schema.createSchema(stringLineitem_join_Orders);

            Globals.save(cfg.getHDFS_WORKING_DIR() + "/" + lineitem_join_orders + "/info",
                    cfg.getHDFS_REPLICATION_FACTOR(), fs);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        rdd = sq.createJoinScanRDD(customer, stringCustomer, q_c, 0, "NULL",lineitem_join_orders, stringLineitem_join_Orders, new Query(new Predicate[0]), 17, cutPoints, memoryBudget);

        result = rdd.count();

        System.out.println("RES: Time Taken: " + (System.currentTimeMillis() - start) + "; Result: " + result);
    }

    public void runWorkload() {

        String dataset1 = "lineitem";
        String dataset2 = "orders";


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


        JavaPairRDD<LongWritable, Text> rdd = sq.createJoinScanRDD(dataset1, stringLineitem,q_l, 0, "NULL", dataset2, stringOrders, q_o, 0, "NULL", memoryBudget);

        /*
        JavaPairRDD<Long, String> mappedRdd = rdd.mapToPair(new PairFunction<Tuple2<LongWritable, Text>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<LongWritable, Text> tuple) throws Exception {
                return new Tuple2(tuple._1().get(), tuple._2().toString());
            }
        });
        */

    }


    public static void main(String[] args) {

        BenchmarkSettings.loadSettings(args);
        BenchmarkSettings.printSettings();

        TPCHJoin t = new TPCHJoin();
        t.loadSettings(args);
        t.setUp();
        t.tpch3();

        //t.runWorkload();
    }
}
