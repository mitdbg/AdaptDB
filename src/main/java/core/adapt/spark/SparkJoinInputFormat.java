package core.adapt.spark;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import core.adapt.Query;
import core.adapt.iterator.*;
import core.common.globals.Globals;
import core.common.globals.Schema;
import core.common.index.MDIndex;
import core.common.index.RobustTree;
import core.utils.HDFSUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


import core.adapt.AccessMethod;
import core.adapt.AccessMethod.PartitionSplit;
import core.utils.ReflectionUtils;
import org.apache.hadoop.mapreduce.lib.join.ArrayListBackedIterator;
import org.apache.hadoop.util.StringUtils;
import scala.Int;


/**
 * Created by ylu on 12/2/15.
 */


public class SparkJoinInputFormat extends
        FileInputFormat<LongWritable, IteratorRecord> implements Serializable {

    public static String SPLIT_ITERATOR = "SPLIT_ITERATOR";
    private static final long serialVersionUID = 1L;

    private static final Log LOG = LogFactory.getLog(FileInputFormat.class);


    private Map<Integer, MDIndex.BucketInfo> dataset1_bucketInfo;
    private Map<Integer, MDIndex.BucketInfo> dataset2_bucketInfo;

    private Map<Integer, ArrayList<Integer> > overlap_chunks;

    private String dataset1, dataset2;
    private Query dataset1_query, dataset2_query;

    private int budget;

    SparkQueryConf queryConf;

    public static class SparkFileSplit extends CombineFileSplit implements
            Serializable {
        private static final long serialVersionUID = 1L;

        private PartitionIterator iterator;

        public SparkFileSplit() {
        }

        public SparkFileSplit(Path[] files, long[] lengths,
                              PartitionIterator iterator) {
            super(files, lengths);
            this.iterator = iterator;
        }

        public SparkFileSplit(Path[] files, long[] start, long[] lengths,
                              String[] locations, PartitionIterator iterator) {
            super(files, start, lengths, locations);
            this.iterator = iterator;
        }

        public PartitionIterator getIterator() {
            return this.iterator;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            super.write(out);
            Text.writeString(out, iterator.getClass().getName());
            iterator.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            super.readFields(in);
            iterator = (PartitionIterator) ReflectionUtils.getInstance(Text
                    .readString(in));
            iterator.readFields(in);
        }

        public static SparkFileSplit read(DataInput in) throws IOException {
            SparkFileSplit s = new SparkFileSplit();
            s.readFields(in);
            return s;
        }
    }

    private void read_index(Map<Integer, MDIndex.BucketInfo> info, String path, int attr){
        String index = path + "/index";
        String sample = path + "/sample";
        FileSystem fs = HDFSUtils.getFS(queryConf.getHadoopHome()
                + "/etc/hadoop/core-site.xml");

        byte[] indexBytes = HDFSUtils.readFile(fs, index);
        RobustTree rt = new RobustTree();
        rt.unmarshall(indexBytes);

        byte[] sampleBytes = HDFSUtils.readFile(fs, sample);
        rt.loadSample(sampleBytes);

        Map<Integer, MDIndex.BucketInfo> bucketRanges = rt.getBucketRanges(attr);

        for(Integer id : bucketRanges.keySet()){
            MDIndex.BucketInfo bi = bucketRanges.get(id);
            info.put(id, bi);
        }
    }

    private void read_range(Map<Integer, MDIndex.BucketInfo> info, String path, int attr) {
        String index = path + "/index";
        // TODO
    }

    private void init_bucketInfo(JobContext job, int[] dataset1_splits, int[] dataset2_splits){
        dataset1_bucketInfo = new HashMap<Integer, MDIndex.BucketInfo>();
        dataset2_bucketInfo = new HashMap<Integer, MDIndex.BucketInfo>();

        overlap_chunks = new HashMap<Integer, ArrayList<Integer> >();

        System.out.println(job.getConfiguration().get("JOIN_ATTR1") + " $$$ " + job.getConfiguration().get("JOIN_ATTR2"));

        int join_attr1 = Integer.parseInt(job.getConfiguration().get("JOIN_ATTR1"));
        int join_attr2  = Integer.parseInt(job.getConfiguration().get("JOIN_ATTR2"));

        int dataset1_type = Integer.parseInt(job.getConfiguration().get("DATASET1_TYPE"));
        int dataset2_type = Integer.parseInt(job.getConfiguration().get("DATASET2_TYPE"));

        //System.out.println(">>>> " + Globals.schema);

        Configuration conf = job.getConfiguration();

        String dataset1_schema = conf.get("DATASET1_SCHEMA");
        String dataset2_schema = conf.get("DATASET2_SCHEMA");


        Globals.schema =  Schema.createSchema(dataset1_schema);

        if(dataset1_type == 0){
            read_index(dataset1_bucketInfo, queryConf.getWorkingDir() + "/" + dataset1,join_attr1);
        } else {
            read_range(dataset1_bucketInfo, queryConf.getWorkingDir() + "/" + dataset1,join_attr1);
        }

        Globals.schema =  Schema.createSchema(dataset2_schema);

        if(dataset2_type == 0){
            read_index(dataset2_bucketInfo, queryConf.getWorkingDir() + "/" + dataset2,join_attr2);
        } else {
            read_range(dataset2_bucketInfo, queryConf.getWorkingDir() + "/" + dataset2,join_attr2);
        }

        // filtering

        HashSet<Integer> splits1 = new HashSet<Integer>();
        for(int i = 0; i < dataset1_splits.length; i ++){
            splits1.add(dataset1_splits[i]);
        }

        HashSet<Integer> splits2 = new HashSet<Integer>();
        for(int i = 0; i < dataset2_splits.length; i ++){
            splits2.add(dataset2_splits[i]);
        }

        /*
        System.out.println(">>> dataset1_bucketInfo");

        for(Integer i : dataset1_bucketInfo.keySet())
        {
            MDIndex.BucketInfo info_i = dataset1_bucketInfo.get(i);
            System.out.println(info_i);
        }

        System.out.println(">>> dataset2_bucketInfo");

        for(Integer i : dataset2_bucketInfo.keySet())
        {
            MDIndex.BucketInfo info_i = dataset2_bucketInfo.get(i);
            System.out.println(info_i);
        }
        */

        for(Integer i : dataset1_bucketInfo.keySet()){
            if(splits1.contains(i) == false) continue;

            MDIndex.BucketInfo info_i = dataset1_bucketInfo.get(i);

            for(Integer j : dataset2_bucketInfo.keySet()){
                if(splits2.contains(j) == false) continue;

                MDIndex.BucketInfo info_j = dataset2_bucketInfo.get(j);
                if(info_i.intersectionFraction(info_j) > 0) { // ? is this right?
                    if (overlap_chunks.containsKey(i) == false){
                        overlap_chunks.put(i, new ArrayList<Integer>());
                    }
                    overlap_chunks.get(i).add(j);
                }
            }
        }

    }

    private int[] expandPartitionSplit(PartitionSplit[] splits){
        int totalLength = 0;
        for(int i = 0 ;i < splits.length; i ++){
            totalLength += splits[i].getPartitions().length;
        }
        int[] partitions = new int[totalLength];
        for(int i = 0, k = 0 ;i < splits.length; i ++){
            int[] par =  splits[i].getPartitions();
            for(int j = 0; j < par.length; j ++)
            {
                partitions[k++] = par[j];
            }
        }
        return partitions;
    }

    private ArrayList<Integer> getOverlappedSplits(ArrayList<Integer> split){
        ArrayList<Integer> final_split = new ArrayList<Integer>();
        HashSet<Integer> overlappedSplits = new HashSet<Integer>();
        for(int i = 0 ;i < split.size(); i ++){
            int id = split.get(i);
            if(overlap_chunks.containsKey(id)){
                overlappedSplits.addAll(overlap_chunks.get(id));
            }
        }
        final_split.addAll(overlappedSplits);
        return final_split;
    }

    public SparkFileSplit formSplits(Path[] paths1, long[] lengths1, Path[] paths2, long[] lengths2, PartitionIterator itr) {
        int totalLength = paths1.length + paths2.length;
        Path[] paths = new Path[totalLength];
        long[] lengths = new long[totalLength];
        for (int i = 0; i < totalLength; i++) {
            if (i < paths1.length) {
                paths[i] = paths1[i];
                lengths[i] = lengths1[i];
            } else {
                paths[i] = paths2[i - paths1.length];
                lengths[i] = lengths2[i - paths1.length];
            }
        }
        return new SparkFileSplit(paths, lengths, itr);
    }

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {

        System.out.println("INFO: in getSplits");

        List<InputSplit> finalSplits = new ArrayList<InputSplit>();

        Configuration conf = job.getConfiguration();

        queryConf = new SparkQueryConf(conf);

        budget = Integer.parseInt(conf.get("BUDGET"));

        dataset1_query = new Query(conf.get("DATASET1_QUERY"));
        dataset2_query = new Query(conf.get("DATASET2_QUERY"));


        dataset1 = conf.get("DATASET1");
        dataset2 = conf.get("DATASET2");

        String workingDir = queryConf.getWorkingDir();
        System.out.println("INFO working dir: " + workingDir);

        AccessMethod dataset1_am = new AccessMethod();
        AccessMethod dataset2_am = new AccessMethod();

        HPJoinInput dataset1_hpinput = new HPJoinInput();
        HPJoinInput dataset2_hpinput = new HPJoinInput();

        //System.out.println("Barrier 0");

        queryConf.setQuery(dataset1_query.getPredicates());
        queryConf.setWorkingDir(workingDir + "/" + dataset1);
        conf.set(INPUT_DIR, workingDir + "/" + dataset1 + "/data");
        dataset1_am.init(queryConf);
        dataset1_hpinput.initialize(listStatus(job), dataset1_am);

        //System.out.println("Barrier 1");

        queryConf.setQuery(dataset2_query.getPredicates());
        queryConf.setWorkingDir(workingDir + "/" + dataset2);
        conf.set(INPUT_DIR, workingDir + "/" + dataset2 + "/data");
        dataset2_am.init(queryConf);
        dataset2_hpinput.initialize(listStatus(job), dataset2_am);

        //System.out.println("Barrier 2");


        int[] dataset1_splits = expandPartitionSplit(dataset1_am.getPartitionSplits(dataset1_query, true));
        int[] dataset2_splits = expandPartitionSplit(dataset2_am.getPartitionSplits(dataset2_query, true));

        //System.out.println("Barrier 3");

        queryConf.setWorkingDir(workingDir);

        init_bucketInfo(job, dataset1_splits, dataset2_splits);

        //System.out.println("Barrier 4");

        JoinAlgo joinAlgo = new RandomGroup();

        ArrayList<ArrayList<Integer> > splits = joinAlgo.getSplits(dataset1_splits, overlap_chunks, budget);

        //System.out.println("Barrier 5");

        int it = 0;

        for (ArrayList<Integer> split : splits) {
            PartitionIterator itr = new PostFilterIterator(dataset1_query);
            ArrayList<Integer> dep_split = getOverlappedSplits(split);

            System.out.println(">>> split");

            for(int i = 0 ;i < split.size() ; i ++){
                System.out.print(split.get(i) + " ");
            }

            System.out.println("\n>>> dep split");

            for(int i = 0 ;i < dep_split.size() ; i ++){
                System.out.print(dep_split.get(i) + " ");
            }

            System.out.println();

            Path[] paths1 = dataset1_hpinput.getPaths(split);
            long[] lens1 = dataset1_hpinput.getLengths(split);
            Path[] paths2 = dataset2_hpinput.getPaths(dep_split);
            long[] lens2 = dataset2_hpinput.getLengths(dep_split);


            System.out.println("Split " + it++);

            for(int i = 0 ;i < paths1.length ;i ++){
                System.out.println(paths1[i]);
            }

            for(int i = 0 ;i < paths2.length ;i ++){
                System.out.println(paths2[i]);
            }

            SparkFileSplit thissplit = formSplits(paths1, lens1, paths2, lens2, itr);
            finalSplits.add(thissplit);
        }

        job.getConfiguration().setLong(NUM_INPUT_FILES, dataset1_splits.length);
        LOG.debug("Total # of splits: " + finalSplits.size());
        System.out.println("done with getting splits");

        return finalSplits;
    }

    @Override
    public RecordReader<LongWritable, IteratorRecord> createRecordReader(
            InputSplit arg0, TaskAttemptContext arg1) throws IOException,
            InterruptedException {
        return new SparkJoinRecordReader();
    }
}
