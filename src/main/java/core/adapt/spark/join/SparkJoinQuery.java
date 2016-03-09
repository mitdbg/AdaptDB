package core.adapt.spark.join;


import core.adapt.AccessMethod.PartitionSplit;
import core.adapt.JoinQuery;
import core.adapt.iterator.PartitionIterator;
import core.adapt.iterator.PostFilterIterator;
import core.utils.HDFSUtils;
import core.utils.RangePartitionerUtils;
import core.utils.SparkUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import core.utils.ConfUtils;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ylu on 1/6/16.
 */


public class SparkJoinQuery {

    public static class Mapper implements PairFunction<Tuple2<LongWritable, Tuple2<Text, Text>>, LongWritable, Text> {
        String Delimiter;
        String Splitter;
        int partitionKey;
        public Mapper(String Delimiter, int partitionKey) {
            this.Delimiter = Delimiter;
            this.partitionKey = partitionKey;
            if (Delimiter.equals("|"))
                Splitter = "\\|";
            else
                Splitter = Delimiter;
        }

        @Override
        public Tuple2<LongWritable, Text> call(Tuple2<LongWritable, Tuple2<Text, Text>> x) throws Exception {
            String s1 = x._2()._1().toString();
            String s2 = x._2()._2().toString();
            String value = s1 + Delimiter + s2;
            long key = Long.parseLong(value.split(Splitter)[partitionKey]);
            return new Tuple2<LongWritable, Text>(new LongWritable(key), new Text(value));
        }
    }

    protected SparkJoinQueryConf queryConf;
    protected JavaSparkContext ctx;
    protected ConfUtils cfg;

    private String Delimiter = "|";
    private String joinStrategy = "Heuristic";

    public SparkJoinQuery(ConfUtils config) {
        this.cfg = config;
        SparkConf sconf = SparkUtils.getSparkConf(this.getClass().getName(), config);

        try {
            sconf.registerKryoClasses(new Class<?>[]{
                    Class.forName("org.apache.hadoop.io.LongWritable"),
                    Class.forName("org.apache.hadoop.io.Text")
            });
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        ctx = new JavaSparkContext(sconf);
        ctx.hadoopConfiguration().setBoolean(
                FileInputFormat.INPUT_DIR_RECURSIVE, true);
        ctx.hadoopConfiguration().set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        queryConf = new SparkJoinQueryConf(ctx.hadoopConfiguration());

        queryConf.setHDFSReplicationFactor(cfg.getHDFS_REPLICATION_FACTOR());
        queryConf.setHadoopHome(cfg.getHADOOP_HOME());
        queryConf.setZookeeperHosts(cfg.getZOOKEEPER_HOSTS());
        queryConf.setMaxSplitSize(1L * 1024 * 1024 * 1024); // 8 GB


    }

    public void setDelimiter(String delimiter) {
        Delimiter = delimiter;
    }

    public String getDelimiter() {
        return Delimiter;
    }

	/* SparkJoinQuery */

    public JavaPairRDD<LongWritable, Text> createJoinRDD(String hdfsPath) {
        queryConf.setReplicaId(0);
        queryConf.setJustAccess(true);
        return ctx.newAPIHadoopFile(cfg.getHADOOP_NAMENODE() + hdfsPath,
                SparkJoinInputFormat.class, LongWritable.class,
                Text.class, ctx.hadoopConfiguration());
    }


    public JavaPairRDD<LongWritable, Text> createJoinRDD(
            String dataset1, JoinQuery dataset1_query, String dataset1_cutpoints,
            String dataset2, JoinQuery dataset2_query, String dataset2_cutpoints,
            int partitionKey) {

        // set SparkQuery conf

        String hdfsPath = cfg.getHDFS_WORKING_DIR();

        queryConf.setWorkingDir(hdfsPath);
        queryConf.setJustAccess(true);

        Configuration conf = ctx.hadoopConfiguration();

        conf.set("DATASET1", dataset1);
        conf.set("DATASET2", dataset2);

        conf.set("DATASET1_CUTPOINTS", dataset1_cutpoints);
        conf.set("DATASET2_CUTPOINTS", dataset2_cutpoints);

        conf.set("DATASET1_QUERY", dataset1_query.toString());
        conf.set("DATASET2_QUERY", dataset2_query.toString());

        conf.set("PARTITION_KEY", Integer.toString(partitionKey)) ;

        conf.set("JOINALGO", joinStrategy);

        conf.set("DELIMITER", Delimiter);

        JoinPlanner planner = new JoinPlanner(conf);

        JavaPairRDD<LongWritable, Text> rdd = null;

        if (planner.hyperjoin){
            // hyper join
            String hyperJoinInput = planner.getHyperJoinInput();
            System.out.println("hyperJoinInput: " + hyperJoinInput);
            conf.set("DATASETINFO",hyperJoinInput);
            rdd = createJoinRDD(hdfsPath);

        } else {
            // shuffle join

            String input1 = planner.getShuffleJoinInput1();
            String input2 = planner.getShuffleJoinInput2();

            System.out.println("shuffleInput1: " + input1);
            System.out.println("shuffleInput2: " + input2);

            // set conf input;
            conf.set("DATASETFLAG", "1");
            conf.set("DATASETINFO", input1);

            JavaPairRDD<LongWritable, Text> dataset1RDD = createSingleTableRDD(hdfsPath, dataset1_query);

            // set conf input;
            conf.set("DATASETFLAG", "2");
            conf.set("DATASETINFO", input2);

            JavaPairRDD<LongWritable, Text> dataset2RDD = createSingleTableRDD(hdfsPath, dataset2_query);

            rdd = dataset1RDD.join(dataset2RDD).mapToPair(new Mapper(Delimiter, partitionKey));

        }

        return rdd;
    }

    /* SingleTableScan  */

    public JavaPairRDD<LongWritable, Text> createSingleTableRDD(String hdfsPath,
                                                                JoinQuery q) {
        queryConf.setWorkingDir(hdfsPath);
        queryConf.setJoinQuery(q);
        queryConf.setJustAccess(true);
        return ctx.newAPIHadoopFile(cfg.getHADOOP_NAMENODE() + hdfsPath + "/" + q.getTable() + "/data",
                SparkScanInputFormat.class, LongWritable.class,
                Text.class, ctx.hadoopConfiguration());
    }

    /* for tpch 6 */

    public JavaPairRDD<LongWritable, Text> createScanRDD(String dataset, JoinQuery q) {

        // set SparkQuery conf

        String hdfsPath = cfg.getHDFS_WORKING_DIR();

        queryConf.setWorkingDir(hdfsPath);

        Configuration conf = ctx.hadoopConfiguration();
        FileSystem fs = HDFSUtils.getFSByHadoopHome(queryConf.getHadoopHome());


        String workingDir = queryConf.getWorkingDir();
        //System.out.println("INFO working dir: " + workingDir);

        JoinAccessMethod dataset1_am = new JoinAccessMethod();
        queryConf.setJoinQuery(q);

        PartitionSplit[] dataset_splits;
        dataset1_am.init(queryConf);


        HPJoinInput dataset_hpinput = new HPJoinInput(true);
        String input_dir = workingDir + "/" + dataset + "/data";
        dataset_hpinput.initialize(JoinPlanner.listStatus(fs, input_dir), dataset1_am);

        // optimize for the JoinRobustTree

        System.out.println("Optimizing dataset");

        dataset_splits = dataset_hpinput.getIndexScan(queryConf.getJustAccess(), q);

        ArrayList<PartitionSplit> shuffleJoinSplit = new  ArrayList<PartitionSplit>();

        for (int i = 0; i < dataset_splits.length; i++) {
            PartitionSplit split = dataset_splits[i];
            int[] bids = split.getPartitions();

            ArrayList<Integer> shuffle_ids = new ArrayList<Integer>();

            for (int j = 0; j < bids.length; j++) {
                shuffle_ids.add(bids[j]);
            }

            if (shuffle_ids.size() > 0) {
                int[] shuffle_ids_int = new int[shuffle_ids.size()];
                for (int j = 0; j < shuffle_ids_int.length; j++) {
                    shuffle_ids_int[j] = shuffle_ids.get(j);
                }
                ArrayList<PartitionSplit> shuffle_splits = JoinPlanner.resizeSplits(split.getIterator(), shuffle_ids_int, dataset_hpinput.getPartitionIdSizeMap(), queryConf.getMaxSplitSize());
                for (PartitionSplit hs : shuffle_splits) {
                    shuffleJoinSplit.add(hs);
                }
            }
        }


        StringBuilder sb = new StringBuilder();

        for (PartitionSplit split : shuffleJoinSplit) {
            if (sb.length() > 0) {
                sb.append(";");
            }

            PartitionIterator iter = split.getIterator();
            if (iter instanceof PostFilterIterator) {
                sb.append(1 + ",");
            } else {
                sb.append(2 + ",");
            }

            int[] bucketIds = split.getPartitions();

            long[] bucket_lens = dataset_hpinput.getLengths(bucketIds);

            for (int i = 0; i < bucketIds.length; i++) {
                if (i > 0) {
                    sb.append(",");
                }
                sb.append(bucketIds[i] + ":" + bucket_lens[i]);
            }
        }

        String input = sb.toString();

        System.out.println("shuffleInput: " + input);

        // set conf input;
        conf.set("DATASETFLAG", "1");
        conf.set("DATASETINFO", input);

        JavaPairRDD<LongWritable, Text> rdd = createSingleTableRDD(hdfsPath, q);

        return rdd;
    }
}
