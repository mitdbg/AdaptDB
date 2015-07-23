package core.access.spark.join;

import core.access.spark.SparkQueryConf;
import core.access.spark.join.algo.HyperJoinOverlappingRanges;
import core.access.spark.join.algo.JoinAlgo;
import core.utils.ConfUtils;
import core.utils.HDFSUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import perf.benchmark.BenchmarkSettings;

import java.util.Arrays;
import java.util.List;

/**
 * Created by qui on 7/22/15.
 */
public class TestJoinAlgo {

    String hdfsPath1 = "/user/qui/copy";
    String hdfsPath2 = "/user/qui/repl";
    int rid1 = 0;
    int joinAttribute1 = 0;
    int rid2 = 0;
    int joinAttribute2 = 0;
    FileSystem fs;

    public List<InputSplit> getSplits(Class<? extends JoinAlgo> algoClass) throws Exception {
        ConfUtils cfg = new ConfUtils(BenchmarkSettings.cartilageConf);
        Configuration conf = new Configuration();

        conf.set("JOIN_INPUT1", hdfsPath1);
        conf.set("JOIN_INPUT2", hdfsPath2);
        conf.set("JOIN_CONDITION", rid1 + "." + joinAttribute1 + "=" + rid2 + "." + joinAttribute2);
        conf.set("HADOOP_NAMENODE", cfg.getHADOOP_NAMENODE());

        SparkQueryConf queryConf = new SparkQueryConf(conf);
        queryConf.setHadoopHome(cfg.getHADOOP_HOME());
        queryConf.setZookeeperHosts(cfg.getZOOKEEPER_HOSTS());
        queryConf.setHDFSReplicationFactor(cfg.getHDFS_REPLICATION_FACTOR());

        fs = HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME());

        Class[] argTypes = new Class[]{HPJoinInput.class, HPJoinInput.class};
        HPJoinInput joinInput1 = new HPJoinInput(conf);
        joinInput1.initialize(
                Arrays.asList(fs.listStatus(new Path(conf.get(FileInputFormat.INPUT_DIR, hdfsPath1)))),
                queryConf);

        HPJoinInput joinInput2 = new HPJoinInput(conf);
        joinInput2.initialize(
                Arrays.asList(fs.listStatus(new Path(conf.get(FileInputFormat.INPUT_DIR, hdfsPath1)))),
                queryConf);

        JoinAlgo algo = algoClass.getConstructor(argTypes).newInstance(new Object[]{joinInput1, joinInput2});
        return algo.getSplits();
    }

    public void analyzeSplits(List<InputSplit> splits) throws Exception {
        System.out.println("Analyzing splits");
        System.out.println("Number of splits: "+splits.size());

        long totalSize = 0;
        for (InputSplit split : splits) {
            totalSize += split.getLength();
        }
        System.out.println("Total input size       "+totalSize);
        System.out.println("-");

        for (InputSplit split : splits) {
            long hashInputSize = 0;
            Path[] files = ((CombineFileSplit)split).getPaths();
            for (Path file : files) {
                if (file.toString().contains(hdfsPath1)) {
                    FileStatus info = fs.getFileStatus(file);
                    hashInputSize += info.getLen();
                }
            }
            System.out.println("Split: hash input size "+hashInputSize);
            System.out.println("Split: total size      "+split.getLength());
            System.out.println("-");
        }
        // could also look at deviation in split sizes
    }

    public static void main(String[] args) {
        TestJoinAlgo test = new TestJoinAlgo();
        try {
            List<InputSplit> splits = test.getSplits(HyperJoinOverlappingRanges.class);
            test.analyzeSplits(splits);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
