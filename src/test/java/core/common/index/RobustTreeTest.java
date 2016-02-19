package core.common.index;

import core.adapt.Predicate;
import core.adapt.Query;
import core.common.globals.Globals;
import core.common.globals.TableInfo;
import core.common.key.ParsedTupleList;
import core.utils.ConfUtils;
import core.utils.HDFSUtils;
import core.utils.TypeUtils;
import org.apache.hadoop.fs.FileSystem;

import java.util.*;

/**
 * Created by ylu on 2/10/16.
 */
public class RobustTreeTest {
    public static void main(String[] args){



        ConfUtils cfg = new ConfUtils("/Users/ylu/Documents/workspace/mdindex/conf/ylu.properties");
        FileSystem fs = HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME());

        byte[] sampleBytes = HDFSUtils.readFile(fs, "/index");


        RobustTree rt = new RobustTree();
        rt.unmarshall(sampleBytes);


        TypeUtils.SimpleDate d3 = new TypeUtils.SimpleDate(1995,
                4, 14);

        Predicate p = new Predicate(10,  TypeUtils.TYPE.DATE, d3, Predicate.PREDTYPE.GT);
        List<RNode> rr = rt.getMatchingBuckets( new Predicate[]{p});
        System.out.println();
    }
}
