package perf.benchmark;


import core.adapt.spark.RangePartitioner;
import core.adapt.spark.SparkQuery;
import core.adapt.spark.SparkQueryConf;
import core.common.globals.Schema;
import core.common.globals.TableInfo;
import core.utils.*;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

import core.adapt.Predicate;
import core.adapt.Query;
import core.adapt.Predicate.PREDTYPE;
import core.utils.TypeUtils.SimpleDate;
import core.utils.TypeUtils.TYPE;

import core.adapt.iterator.IteratorRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import javax.xml.crypto.Data;


/**
 * Created by ylu on 1/4/16.
 */


public class TPCHSparkJoinWorkload {

    private ConfUtils cfg;

    private String lineitem = "lineitem", orders = "orders", customer = "customer", supplier = "supplier", part = "part";

    private static String[] mktSegmentVals = new
            String[]{"AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY"};
    private static String[] regionNameVals = new
            String[]{"AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"};
    private static String[] partTypeVals = new
            String[]{"BRASS", "COPPER", "NICKEL", "STEEL", "TIN"};
    private static String[] shipModeVals = new
            String[]{"AIR", "FOB", "MAIL", "RAIL", "REG AIR", "SHIP", "TRUCK"};

    private SparkQuery sq;

    private String Delimiter = "|";
    private int method;
    private int numQueries;

    private Random rand;

    private JavaSparkContext ctx;
    private SQLContext sqlContext;

    public void setUp() {
        cfg = new ConfUtils(BenchmarkSettings.conf);

        rand = new Random();

        // Making things more deterministic.
        rand.setSeed(0);

        SparkConf sconf = new SparkConf().setMaster(cfg.getSPARK_MASTER())
                .setAppName(this.getClass().getName())
                .setSparkHome(cfg.getSPARK_HOME())
                .setJars(new String[]{cfg.getSPARK_APPLICATION_JAR()})
                .set("spark.hadoop.cloneConf", "false")
                .set("spark.executor.memory", cfg.getSPARK_EXECUTOR_MEMORY())
                .set("spark.driver.memory", cfg.getSPARK_DRIVER_MEMORY())
                .set("spark.task.cpus", cfg.getSPARK_TASK_CPUS());

        try {
            sconf.registerKryoClasses(new Class<?>[]{
                    Class.forName("org.apache.hadoop.io.LongWritable"),
                    Class.forName("org.apache.hadoop.io.Text")
            });
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        ctx = new JavaSparkContext(sconf);
        ctx.hadoopConfiguration().setBoolean(
                FileInputFormat.INPUT_DIR_RECURSIVE, true);
        ctx.hadoopConfiguration().set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());

        sqlContext = new SQLContext(ctx);
    }


    public void loadSettings(String[] args) {
        int counter = 0;
        while (counter < args.length) {
            switch (args[counter]) {
                case "--method":
                    method = Integer.parseInt(args[counter + 1]);
                    counter += 2;
                    break;
                case "--numQueries":
                    numQueries = Integer.parseInt(args[counter + 1]);
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


        String customerPredicate = "c_mktsegment <= \"" +  c_mktsegment + "\"";
        String ordersPredicate = "o_orderdate < \"" + d3 + "\"";
        String lineitemPredicate = "l_shipdate > \"" + d3 + "\"";


        if (rand_3 > 0) {
            String c_mktsegment_prev = mktSegmentVals[rand_3 - 1];
            customerPredicate = "c_mktsegment > \"" + c_mktsegment_prev + "\" and " + customerPredicate;
        }



        long start = System.currentTimeMillis();

        String customerPath = cfg.getHDFS_WORKING_DIR() + "/" + customer + "/data";
        String ordersPath = cfg.getHDFS_WORKING_DIR() + "/" + orders + "/data";
        String lineitemPath = cfg.getHDFS_WORKING_DIR() + "/" + lineitem + "/data";

        // Create customer table

        sqlContext.sql("CREATE TEMPORARY TABLE customer (c_custkey int, c_name string, c_address string, "
                + "c_phone string, c_acctbal double, c_mktsegment string , c_nation string, c_region string) "
                + "USING com.databricks.spark.csv "
                + "OPTIONS (path \"" + customerPath + "\", header \"false\", delimiter \"|\")");

        // Create order table.
        sqlContext.sql("CREATE TEMPORARY TABLE orders (o_orderkey int, o_custkey int, "
                + "o_orderstatus string, o_totalprice double, o_orderdate string, "
                + "o_orderpriority string, o_clerk string, o_shippriority int) "
                + "USING com.databricks.spark.csv "
                + "OPTIONS (path \"" + ordersPath + "\", header \"false\", delimiter \"|\")");


        // Create lineitem table.
        sqlContext.sql("CREATE TEMPORARY TABLE lineitem (l_orderkey int, l_partkey int, l_suppkey int, "
                + "l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, "
                + "l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, "
                + "l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) "
                + "USING com.databricks.spark.csv "
                + "OPTIONS (path \"" + lineitemPath + "\", header \"false\", delimiter \"|\")");


        System.out.println("SELECT  COUNT(*) "
                + "FROM lineitem JOIN orders ON l_orderkey = o_orderkey JOIN customer ON o_custkey = c_custkey "
                + "WHERE " + lineitemPredicate + " and " + ordersPredicate + " and " +  customerPredicate);

        DataFrame df = sqlContext.sql("SELECT  COUNT(*) "
                + "FROM lineitem JOIN orders ON l_orderkey = o_orderkey JOIN customer ON o_custkey = c_custkey "
                + "WHERE " + lineitemPredicate + " and " + ordersPredicate + " and " +  customerPredicate);



        // 29569

        String result = df.collect()[0].toString();

        System.out.println("RES: Time Taken: " + (System.currentTimeMillis() - start)  + "; Result: " + result);

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

        ((customer ⋈ orders) ⋈ lineitem) ⋈ supplier
     */

    public void tpch5() {

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

    public void tpch8() {

    }

    /*
        select
            count(*)
        from
            orders,
            lineitem,
            customer
        where
            l_orderkey = o_orderkey
            and c_custkey = o_custkey
            and o_orderdate >= date '[DATE]'
            and o_orderdate < date '[DATE]' + interval '3' month
            and l_returnflag = 'R'

        (lineitem ⋈ orders) ⋈ customer
     */


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

    public void tpch19() {

    }

    public void runWorkload(int numQueries) {
        int queries[] = {3, 5, 6, 8, 10, 12, 14, 19};

        tpch3();
        if (true)
            return;

        for (int i = 0; i < numQueries; i++) {
            int q = queries[rand.nextInt(queries.length)];
            System.out.println("INFO: Running query " + q);
            switch (q) {
                case 3:
                    tpch3();
                    break;
                case 5:
                    tpch5();
                    break;
                case 6:
                    tpch6();
                    break;
                case 8:
                    tpch8();
                    break;
                case 10:
                    tpch10();
                    break;
                case 12:
                    tpch12();
                    break;
                case 14:
                    tpch14();
                    break;
                case 19:
                    tpch19();
                    break;
            }
        }
    }

    public static void main(String[] args) {

        BenchmarkSettings.loadSettings(args);
        BenchmarkSettings.printSettings();

        TPCHSparkJoinWorkload t = new TPCHSparkJoinWorkload();
        t.loadSettings(args);
        t.setUp();

        switch (t.method) {
            case 1:
                System.out.println("Num Queries: " + t.numQueries);
                t.runWorkload(t.numQueries);
                break;
            default:
                break;
        }

    }
}
