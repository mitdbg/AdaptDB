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

import core.utils.RangePartitionerUtils;
import core.utils.TypeUtils;
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
        FileInputFormat<LongWritable, Text> implements Serializable {

    public static String SPLIT_ITERATOR = "SPLIT_ITERATOR";
    private static final long serialVersionUID = 1L;

    private static final Log LOG = LogFactory.getLog(FileInputFormat.class);


    private Map<Integer, MDIndex.BucketInfo> dataset1_bucketInfo;
    private Map<Integer, MDIndex.BucketInfo> dataset2_bucketInfo;

    private Map<Integer, ArrayList<Integer> > overlap_chunks;

    private String dataset1, dataset2;
    private Query dataset1_query, dataset2_query;

    private int[] dataset1_cutpoints, dataset2_cutpoints;
    private boolean dataset1_MDIndex, dataset2_MDIndex;

    private int budget;

    SparkQueryConf queryConf;

    public static class SparkJoinFileSplit extends CombineFileSplit implements
            Serializable {
        private static final long serialVersionUID = 1L;

        private PartitionIterator iter1, iter2;

        public SparkJoinFileSplit() {
        }

        public SparkJoinFileSplit(Path[] files, long[] lengths,
                              PartitionIterator iter1, PartitionIterator iter2) {
            super(files, lengths);
            this.iter1 = iter1;
            this.iter2 = iter2;
        }

        public PartitionIterator getFirstIterator() {
            return this.iter1;
        }
        public PartitionIterator getSecondIterator() {
            return this.iter2;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            super.write(out);
            Text.writeString(out, iter1.getClass().getName());
            iter1.write(out);
            iter2.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            super.readFields(in);
            String type = Text.readString(in);
            iter1 = (PartitionIterator) ReflectionUtils.getInstance(type);
            iter1.readFields(in);
            iter2 = (PartitionIterator) ReflectionUtils.getInstance(type);
            iter2.readFields(in);
        }

        public static SparkJoinFileSplit read(DataInput in) throws IOException {
            SparkJoinFileSplit s = new SparkJoinFileSplit();
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

        //byte[] sampleBytes = HDFSUtils.readFile(fs, sample);
        //rt.loadSample(sampleBytes);

        // If a particular bucket is not in the following map, then the range is (-oo,+oo).

        Map<Integer, MDIndex.BucketInfo> bucketRanges = rt.getBucketRanges(attr);

        for(int i = 0 ;i < rt.maxBuckets; i ++){
            if (bucketRanges.containsKey(i) == false){
                // hard code, the join key can only be int.
                info.put(i, new MDIndex.BucketInfo(TypeUtils.TYPE.INT, null, null));
            } else {
                info.put(i, bucketRanges.get(i));
            }
        }
    }

    private void read_range(Map<Integer, MDIndex.BucketInfo> info, int[] cutpoints) {
        // [10,200] ~ (-oo, 10] (10, 200] (200, +oo)

        if(cutpoints.length == 0){
            info.put(0, new MDIndex.BucketInfo(TypeUtils.TYPE.INT, null, null));
        } else {
            info.put(0, new MDIndex.BucketInfo(TypeUtils.TYPE.INT, null, cutpoints[0]));
            for(int i = 1;i < cutpoints.length; i ++){
                info.put(i, new MDIndex.BucketInfo(TypeUtils.TYPE.INT, cutpoints[i-1] + 1, cutpoints[i]));
            }
            info.put(cutpoints.length, new MDIndex.BucketInfo(TypeUtils.TYPE.INT, cutpoints[cutpoints.length-1], null));
        }
    }

    private void init_bucketInfo(JobContext job, int[] dataset1_splits, int[] dataset2_splits){
        dataset1_bucketInfo = new HashMap<Integer, MDIndex.BucketInfo>();
        dataset2_bucketInfo = new HashMap<Integer, MDIndex.BucketInfo>();

        overlap_chunks = new HashMap<Integer, ArrayList<Integer> >();

        System.out.println(job.getConfiguration().get("JOIN_ATTR1") + " $$$ " + job.getConfiguration().get("JOIN_ATTR2"));

        int join_attr1 = Integer.parseInt(job.getConfiguration().get("JOIN_ATTR1"));
        int join_attr2  = Integer.parseInt(job.getConfiguration().get("JOIN_ATTR2"));

        //System.out.println(">>>> " + Globals.schema);

        Configuration conf = job.getConfiguration();

        String dataset1_schema = conf.get("DATASET1_SCHEMA");
        String dataset2_schema = conf.get("DATASET2_SCHEMA");


        Globals.schema = Schema.createSchema(dataset1_schema);

        System.out.println("Populate dataset1_bucketInfo");

        if(dataset1_MDIndex){
            read_index(dataset1_bucketInfo, queryConf.getWorkingDir() + "/" + dataset1,join_attr1);
        } else {
            read_range(dataset1_bucketInfo, dataset1_cutpoints);
        }

        Globals.schema =  Schema.createSchema(dataset2_schema);

        System.out.println("Populate dataset2_bucketInfo");

        if(dataset2_MDIndex){
            read_index(dataset2_bucketInfo, queryConf.getWorkingDir() + "/" + dataset2,join_attr2);
        } else {
            read_range(dataset2_bucketInfo, dataset2_cutpoints);
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

                //System.out.println(i + " from " + dataset1 + " intersects with " + j +  " from "+  dataset2 + " result: " + info_i.overlap(info_j));

                if(info_i.overlap(info_j)) {
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

    public SparkJoinFileSplit formSplits(Path[] paths1, long[] lengths1, Path[] paths2, long[] lengths2, PartitionIterator iter1,PartitionIterator iter2 ) {
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
        return new SparkJoinFileSplit(paths, lengths, iter1, iter2);
    }

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {

        System.out.println("INFO: in getSplits");

        List<InputSplit> finalSplits = new ArrayList<InputSplit>();

        Configuration conf = job.getConfiguration();

        queryConf = new SparkQueryConf(conf);

        budget = Integer.parseInt(conf.get("BUDGET"));

        dataset1 = conf.get("DATASET1");
        dataset2 = conf.get("DATASET2");

        dataset1_query = new Query(conf.get("DATASET1_QUERY"));
        dataset2_query = new Query(conf.get("DATASET2_QUERY"));

        dataset1_cutpoints = RangePartitionerUtils.getIntCutPoints(job.getConfiguration().get("DATASET1_CUTPOINTS"));
        dataset2_cutpoints = RangePartitionerUtils.getIntCutPoints(job.getConfiguration().get("DATASET2_CUTPOINTS"));

        dataset1_MDIndex = dataset1_cutpoints == null;
        dataset2_MDIndex = dataset2_cutpoints == null;

        String workingDir = queryConf.getWorkingDir();
        System.out.println("INFO working dir: " + workingDir);

        AccessMethod dataset1_am = new AccessMethod();
        AccessMethod dataset2_am = new AccessMethod();

        // if there are no cutpoints, we read ranges from MDIndex.

        HPJoinInput dataset1_hpinput = new HPJoinInput(dataset1_MDIndex);
        HPJoinInput dataset2_hpinput = new HPJoinInput(dataset2_MDIndex);

        int[] dataset1_splits = null;

        queryConf.setWorkingDir(workingDir + "/" + dataset1);
        conf.set(INPUT_DIR, workingDir + "/" + dataset1 + "/data");
        if(dataset1_MDIndex){
            queryConf.setQuery(dataset1_query.getPredicates());
            dataset1_am.init(queryConf);
            dataset1_hpinput.initialize(listStatus(job), dataset1_am);
            dataset1_splits = expandPartitionSplit(dataset1_am.getPartitionSplits(dataset1_query, true));
        } else {
            dataset1_hpinput.initialize(listStatus(job));
            dataset1_splits = RangePartitionerUtils.getSplits(dataset1_cutpoints);
        }

        int[] dataset2_splits = null;

        queryConf.setWorkingDir(workingDir + "/" + dataset2);
        conf.set(INPUT_DIR, workingDir + "/" + dataset2 + "/data");
        if(dataset2_MDIndex){
            queryConf.setQuery(dataset2_query.getPredicates());
            dataset2_am.init(queryConf);
            dataset2_hpinput.initialize(listStatus(job), dataset2_am);
            dataset2_splits = expandPartitionSplit(dataset2_am.getPartitionSplits(dataset2_query, true));
        } else {
            dataset2_hpinput.initialize(listStatus(job));
            dataset2_splits = RangePartitionerUtils.getSplits(dataset2_cutpoints);
        }

        //System.out.println("Barrier 3");

        queryConf.setWorkingDir(workingDir);

        init_bucketInfo(job, dataset1_splits, dataset2_splits);

        //System.out.println("Barrier 4");

        JoinAlgo joinAlgo = new RandomGroup();

        ArrayList<ArrayList<Integer> > splits = joinAlgo.getSplits(dataset1_splits, overlap_chunks, budget);

        //System.out.println("Barrier 5");


        System.out.println("Bucket range info " + dataset1 + ", " + dataset1_splits.length + " files in total");

        for(int i = 0 ;i  < dataset1_splits.length ; i ++){
            System.out.println(dataset1_splits[i] + " " + dataset1_bucketInfo.get(dataset1_splits[i]));
        }


        System.out.println("Bucket range info " + dataset2 + ", " + dataset2_splits.length + " files in total");

        for(int i = 0 ;i  < dataset2_splits.length ; i ++){
            System.out.println(dataset2_splits[i] + " " + dataset2_bucketInfo.get(dataset2_splits[i]));
        }

        int it = 0;

        PartitionIterator iter1 = new PostFilterIterator(dataset1_query);
        PartitionIterator iter2 = new PostFilterIterator(dataset2_query);

        System.out.println("In SparkJoinInputFormat: " +dataset1_query + " ## " + dataset2_query);


        for (ArrayList<Integer> split : splits) {

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

            SparkJoinFileSplit thissplit = formSplits(paths1, lens1, paths2, lens2, iter1, iter2);
            finalSplits.add(thissplit);
        }

        job.getConfiguration().setLong(NUM_INPUT_FILES, dataset1_splits.length);
        LOG.debug("Total # of splits: " + finalSplits.size());
        System.out.println("done with getting splits");

        return finalSplits;
    }

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(
            InputSplit arg0, TaskAttemptContext arg1) throws IOException,
            InterruptedException {
        return new SparkJoinRecordReader();
    }
}
