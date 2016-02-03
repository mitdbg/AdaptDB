package core.common.globals;

import core.adapt.AccessMethod;
import core.adapt.JoinQuery;
import core.adapt.Predicate;
import core.adapt.opt.JoinOptimizer;
import core.utils.ConfUtils;
import core.utils.HDFSUtils;
import core.utils.TypeUtils;
import org.apache.hadoop.fs.FileSystem;

/**
 * Created by ylu on 2/1/16.
 */
public class TableInfoTest {
    public static void main(String[] args){
        ConfUtils cfg = new ConfUtils("/Users/ylu/Documents/workspace/mdindex/conf/ylu.properties");
        FileSystem fs = HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME());
        String tableName = "lineitem";

        // Load table info.
        Globals.loadTableInfo(tableName, cfg.getHDFS_WORKING_DIR(), fs);
        TableInfo tableInfo = Globals.getTableInfo(tableName);
        tableInfo.gc(cfg.getHDFS_WORKING_DIR(), fs);

    }
}
