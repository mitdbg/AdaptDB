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

/**
 * Created by ylu on 1/21/16.
 */
public class JoinOptimizerTest {
    public static void main(String[] args){
        ConfUtils cfg = new ConfUtils("/Users/ylu/Documents/workspace/mdindex/conf/ylu.properties");
        FileSystem fs = HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME());
        String tableName = "lineitem";
        String lineitem = "l_orderkey int, l_partkey int, l_suppkey int, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate date, l_commitdate date, l_receiptdate date, l_shipinstruct string, l_shipmode string";
        Schema schema = Schema.createSchema(lineitem);
        Predicate p = new Predicate(schema.getAttributeId("l_discount"), TypeUtils.TYPE.DOUBLE, 0.5, Predicate.PREDTYPE.GT);
        JoinQuery q = new JoinQuery(tableName, 0, new Predicate[]{p});

        // Load table info.
        Globals.loadTableInfo(tableName, cfg.getHDFS_WORKING_DIR(), fs);
        TableInfo tableInfo = Globals.getTableInfo(tableName);
        JoinOptimizer opt = new JoinOptimizer(cfg);
        opt.loadIndex(tableInfo);
        opt.printTree();
        opt.loadQueries(tableInfo);

        PartitionSplit[] split = opt.buildPlan(q);

        System.out.println("Hello world");

    }
}
