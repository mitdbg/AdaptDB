package core.adapt.opt;

import core.common.globals.Globals;
import core.common.globals.TableInfo;
import core.utils.ConfUtils;
import core.utils.HDFSUtils;
import org.apache.hadoop.fs.FileSystem;

/**
 * Created by ylu on 1/21/16.
 */
public class JoinOptimizerTest {
    public static void main(String[] args){
        ConfUtils cfg = new ConfUtils("/Users/ylu/Documents/workspace/mdindex/conf/ylu.properties");
        FileSystem fs = HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME());
        String tableName = "lineitem";
        // Load table info.
        Globals.loadTableInfo(tableName, cfg.getHDFS_WORKING_DIR(), fs);
        TableInfo tableInfo = Globals.getTableInfo(tableName);
        JoinOptimizer opt = new JoinOptimizer(cfg);
        opt.loadIndex(tableInfo);
        opt.printTree();

        System.out.println("Hello world");

    }
}
