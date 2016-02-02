package core.upfront.build;

import core.utils.ConfUtils;
import core.utils.HDFSUtils;
import junit.framework.TestCase;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import perf.benchmark.BenchmarkSettings;
import perf.benchmark.CreateTableInfo;
import perf.benchmark.RunIndexBuilder;

import java.io.IOException;

/**
 * Created by anil on 12/18/15.
 */
public class IndexBuilderTest extends TestCase {
    ConfUtils cfg;
    FileSystem fs;

    public void createTableInfo(String[] args) {
        CreateTableInfo cti = new CreateTableInfo();
        cti.loadSettings(args);
        cti.createTableInfo();
    }

    public void testEndToEnd() {
        String homeDir = "/Users/anil/Dev/repos/mdindex";
        String[] args = new String[]{
                "--tableName", "tpch",
                "--numTuples", "30329",
                "--delimiter", "|",
                "--schema", "l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate date, l_commitdate date, l_receiptdate date, l_shipinstruct string, l_shipmode string, o_orderstatus string, o_totalprice double, o_orderdate date, o_orderpriority string, o_clerk string, o_shippriority int, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string",
                "--inputsDir", homeDir + "/data/tpchd/",
                "--samplingRate", "0.1",
                "--numBuckets", "16",
                "--conf", homeDir + "/conf/test.properties"
        };
        BenchmarkSettings.loadSettings(args);

        cfg = new ConfUtils(BenchmarkSettings.conf);
        fs = HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME());

        HDFSUtils.deleteFile(fs, cfg.getHDFS_WORKING_DIR(), true);
        HDFSUtils.safeCreateDirectory(fs, cfg.getHDFS_WORKING_DIR());

        createTableInfo(args);

        RunIndexBuilder t = new RunIndexBuilder();
        t.loadSettings(args);
        t.setUp();

        t.createSamples();
        t.buildRobustTreeFromSamples();
        t.writePartitionsFromIndex();

        String basePath = cfg.getHDFS_WORKING_DIR();
        try {
            assertTrue(fs.exists(new Path(basePath + "/tpch/info")));
            assertTrue(fs.exists(new Path(basePath + "/tpch/sample")));
            assertTrue(fs.exists(new Path(basePath + "/tpch/index")));
            for (int i=0; i<16; i++) {
                assertTrue(fs.exists(new Path(basePath + "/tpch/data/" + i)));
            }
        } catch(IOException e) {
            fail("fs.exists call failed");
        }
    }
}
