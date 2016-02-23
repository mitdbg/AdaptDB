package core.adapt.opt;

import core.adapt.AccessMethod.PartitionSplit;
import core.adapt.JoinQuery;
import core.adapt.Predicate;
import core.common.globals.Globals;
import core.common.globals.Schema;
import core.common.globals.TableInfo;
import core.utils.ConfUtils;
import core.utils.HDFSUtils;
import core.utils.TypeUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;

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

    private static JoinQuery tpch12() {
        int rand_12 = rand.nextInt(shipModeVals.length);
        String shipmode_12 = shipModeVals[rand_12];
        int year_12 = 1993 + rand.nextInt(5);
        TypeUtils.SimpleDate d12_1 = new TypeUtils.SimpleDate(year_12, 1, 1);
        TypeUtils.SimpleDate d12_2 = new TypeUtils.SimpleDate(1997, 2, 24);
        Predicate p1_12 = new Predicate(schemaLineitem.getAttributeId("l_shipmode"), TypeUtils.TYPE.STRING, shipmode_12, Predicate.PREDTYPE.LEQ);
        Predicate p2_12 = new Predicate(schemaLineitem.getAttributeId("l_receiptdate"), TypeUtils.TYPE.DATE, d12_1, Predicate.PREDTYPE.GEQ);
        Predicate p3_12 = new Predicate(schemaLineitem.getAttributeId("l_receiptdate"), TypeUtils.TYPE.DATE, d12_2, Predicate.PREDTYPE.LT);

        JoinQuery q_l = null;

        if (rand_12 > 0) {
            String shipmode_prev_12 = shipModeVals[rand_12 - 1];
            Predicate p4_12 = new Predicate(schemaLineitem.getAttributeId("l_shipmode"), TypeUtils.TYPE.STRING, shipmode_prev_12, Predicate.PREDTYPE.GT);
            q_l = new JoinQuery(lineitem, schemaLineitem.getAttributeId("l_orderkey"), new Predicate[]{p1_12, p2_12, p3_12, p4_12});
            //q_l = new JoinQuery(lineitem, schemaLineitem.getAttributeId("l_orderkey"), new Predicate[]{p3_12});
        } else {
            q_l = new JoinQuery(lineitem, schemaLineitem.getAttributeId("l_orderkey"), new Predicate[]{p1_12, p2_12, p3_12});
            //q_l = new JoinQuery(lineitem, schemaLineitem.getAttributeId("l_orderkey"), new Predicate[]{p3_12});
        }

        System.out.println("INFO: Query_lineitem:" + q_l.toString());

        return q_l;

    }

    public static void main(String[] args) {
        setup();

        ConfUtils cfg = new ConfUtils("/Users/ylu/Documents/workspace/mdindex/conf/ylu.properties");
        FileSystem fs = HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME());
        Globals.loadTableInfo(lineitem, cfg.getHDFS_WORKING_DIR(), fs);
        TableInfo tableInfo = Globals.getTableInfo(lineitem);

/*
        JoinQuery[] qs = new JoinQuery[5];
        for (int i = 0; i < 5; i++) {
            qs[i] = tpch12();
        }


        for (int i = 0; i < 10; i++) {
            for (int k = 0; k < 5; k++) {
                JoinQuery q = qs[k];
                System.out.printf("Query %d: %s\n", k, q);
                // Load table info.

                JoinOptimizer opt = new JoinOptimizer(cfg);
                opt.loadIndex(tableInfo);
                opt.loadQueries(tableInfo);
                PartitionSplit[] split = opt.buildPlan(q);
            }
        }
*/

        for (int i = 0; i < 50; i++) {

            JoinQuery q = tpch12();
            System.out.printf("Query %d: %s\n", i, q);
            // Load table info.

            JoinOptimizer opt = new JoinOptimizer(cfg);
            opt.loadIndex(tableInfo);
            opt.loadQueries(tableInfo);
            PartitionSplit[] split = opt.buildPlan(q);

        }
    }
}
