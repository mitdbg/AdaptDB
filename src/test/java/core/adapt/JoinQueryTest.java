package core.adapt;

import core.common.globals.Schema;
import core.utils.TypeUtils;

/**
 * Created by ylu on 1/25/16.
 */
public class JoinQueryTest {
    public static void main(String[] args){
        String lineitem = "l_orderkey int, l_partkey int, l_suppkey int, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate date, l_commitdate date, l_receiptdate date, l_shipinstruct string, l_shipmode string";
        Schema schema = Schema.createSchema(lineitem);
        Predicate p = new Predicate(schema.getAttributeId("l_discount"), TypeUtils.TYPE.DOUBLE, 0.5, Predicate.PREDTYPE.GT);
        JoinQuery q = new JoinQuery("lineitem",10, new Predicate[]{p});
        String qs = q.toString();
        System.out.println(qs);
        JoinQuery q2 = new JoinQuery(qs);
        System.out.println(q2.toString());

    }
}
