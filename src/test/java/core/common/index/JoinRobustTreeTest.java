package core.common.index;

import core.common.globals.Globals;
import core.common.globals.TableInfo;
import core.common.key.ParsedTupleList;
import core.utils.ConfUtils;
import core.utils.HDFSUtils;
import org.apache.hadoop.fs.FileSystem;

import java.util.Arrays;

/**
 * Created by ylu on 1/21/16.
 */

public class JoinRobustTreeTest {
    public static void main(String[] args){

        ConfUtils cfg = new ConfUtils("/Users/ylu/Documents/workspace/mdindex/conf/ylu.properties");
        FileSystem fs = HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME());
        String tableName = "lineitem";
        // Load table info.
        Globals.loadTableInfo(tableName, cfg.getHDFS_WORKING_DIR(), fs);
        TableInfo tableInfo = Globals.getTableInfo(tableName);

        String pathToSample = cfg.getHDFS_WORKING_DIR()+ "/" + tableInfo.tableName + "/sample";

        byte[] sampleBytes = HDFSUtils.readFile(fs, pathToSample);

        // read all the sample files and put them into the sample key set
        ParsedTupleList sample = new ParsedTupleList(tableInfo.getTypeArray());
        sample.unmarshall(sampleBytes, tableInfo.delimiter);


        JoinRobustTree rt = new JoinRobustTree(tableInfo);
        int numBuckets = 16;
        rt.setMaxBuckets(numBuckets);

        rt.loadSample(sample);
        rt.initProbe();

        double[] alloc = rt.getAllocations();
        System.out.println(Arrays.toString(alloc));
    }
}