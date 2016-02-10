package core.util;

import core.common.globals.Globals;
import core.common.globals.Schema;
import core.common.globals.TableInfo;
import core.utils.ConfUtils;
import core.utils.HDFSUtils;
import core.utils.RangePartitionerUtils;
import org.apache.hadoop.fs.FileSystem;

import java.util.ArrayList;

/**
 * Created by ylu on 2/10/16.
 */
public class RangePartitionerUtilsTest {
    public static void main(String[] args) {
        ConfUtils cfg = new ConfUtils("/Users/ylu/Documents/workspace/mdindex/conf/ylu.properties");
        FileSystem fs = HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME());
        String lineitem = "lineitem";
        // Load table info.
        Globals.loadTableInfo(lineitem, cfg.getHDFS_WORKING_DIR(), fs);
        TableInfo tableInfo = Globals.getTableInfo(lineitem);

        ArrayList<Long> l_orderkey_keys = RangePartitionerUtils.getKeys(cfg, tableInfo, cfg.getHDFS_WORKING_DIR() + "/"  + lineitem + "/sample", 0);

        System.out.println();
    }
}
