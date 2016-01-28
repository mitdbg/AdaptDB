package core.adapt.spark.join;

import core.adapt.JoinQuery;
import core.adapt.Predicate;
import core.common.globals.Schema;
import core.utils.TypeUtils.SimpleDate;
import core.utils.TypeUtils.TYPE;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.catalyst.expressions.Rand;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Random;

/**
 * Created by ylu on 1/27/16.
 */
public class JoinPlannerTest {

    public static void main(String[] args){

        Random rand = new Random();
        rand.setSeed(0);

        String lineitem = "l_orderkey int, l_partkey int, l_suppkey int, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate date, l_commitdate date, l_receiptdate date, l_shipinstruct string, l_shipmode string";
        Schema lineitemSchema = Schema.createSchema(lineitem);
        Predicate p1 = new Predicate(lineitemSchema.getAttributeId("l_discount"), TYPE.DOUBLE, 0.5, Predicate.PREDTYPE.GT);
        JoinQuery q1 = new JoinQuery("lineitem", 0, new Predicate[]{p1});

        String orders = "o_orderkey int, o_custkey int, o_orderstatus string, o_totalprice double, o_orderdate date, o_orderpriority string, o_clerk string, o_shippriority int";
        Schema ordersSchema =  Schema.createSchema(orders);
        Calendar c = new GregorianCalendar();
        int dateOffset = (int) (rand.nextFloat() * (31 + 28 + 31));
        c.set(1995, Calendar.MARCH, 01);
        c.add(Calendar.DAY_OF_MONTH, dateOffset);
        SimpleDate d3 = new SimpleDate(c.get(Calendar.YEAR),
                c.get(Calendar.MONTH), c.get(Calendar.DAY_OF_MONTH));


        Predicate p2 = new Predicate(ordersSchema.getAttributeId("o_orderdate"), TYPE.DATE, d3, Predicate.PREDTYPE.LT);
        JoinQuery q2 = new JoinQuery("orders", 0, new Predicate[]{p2});

        Configuration conf = new Configuration();

        conf.set("DATASET1", "lineitem");
        conf.set("DATASET2", "orders");

        conf.set("DATASET1_CUTPOINTS", "NULL");
        conf.set("DATASET2_CUTPOINTS", "NULL");

        conf.set("DATASET1_QUERY", q1.toString());
        conf.set("DATASET2_QUERY", q2.toString());

        conf.set("JOIN_ATTR1", Integer.toString(q1.getJoinAttribute()));
        conf.set("JOIN_ATTR2", Integer.toString(q2.getJoinAttribute()));
        conf.set("PARTITION_KEY", Integer.toString(0)) ;

        conf.set("JOINALGO", "Heuristic");

        conf.set("DELIMITER", "|");

        conf.set("HADOOP_HOME", "/Users/ylu/Documents/workspace/hadoop-2.6.0");
        conf.set("WORKING_DIR", "/user/ylu");
        conf.set("HDFS_REPLICATION_FACTOR", "0");
        conf.set("MAX_SPLIT_SIZE", "1000000000");
        conf.set("MIN_SPLIT_SIZE", "1000000000");
        conf.set("JOIN_ATTR1", "0");
        conf.set("JOIN_ATTR2", "0");
        JoinPlanner planner = new JoinPlanner(conf);

        System.out.println("");
    }
}
