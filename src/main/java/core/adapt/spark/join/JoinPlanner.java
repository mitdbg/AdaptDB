package core.adapt.spark.join;


import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.primitives.Ints;
import core.adapt.AccessMethod;
import core.adapt.Query;

import core.adapt.iterator.PartitionIterator;
import core.adapt.iterator.PostFilterIterator;
import core.adapt.spark.SparkQueryConf;
import core.common.index.MDIndex;
import core.common.index.RobustTree;

import core.utils.HDFSUtils;
import core.utils.RangePartitionerUtils;
import core.utils.TypeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import core.adapt.AccessMethod.PartitionSplit;
import scala.Int;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.*;

/**
 * Created by ylu on 1/5/16.
 */
public class JoinPlanner {
    FileSystem fs;

    private SparkQueryConf queryConf;
    private String dataset1, dataset2;
    private Query dataset1_query, dataset2_query;
    private int[] dataset1_cutpoints, dataset2_cutpoints;
    private boolean dataset1_MDIndex, dataset2_MDIndex;
    private RobustTree rt1, rt2;

    private Map<Integer, MDIndex.BucketInfo> dataset1_bucketInfo;
    private Map<Integer, MDIndex.BucketInfo> dataset2_bucketInfo;
    private PartitionSplit[] dataset1_splits;
    private int[] dataset1_int_splits, dataset2_int_splits;

    private Map<Integer, ArrayList<Integer>> overlap_chunks;
    private HPJoinInput dataset1_hpinput, dataset2_hpinput;

    private ArrayList<PartitionSplit> hyperJoinSplit, shuffleJoinSplit;

    private int threshold = 80000000;


    public JoinPlanner(Configuration conf) {

        System.out.println("INFO: in JoinPlanner constructor");

        queryConf = new SparkQueryConf(conf);

        fs = HDFSUtils.getFS(queryConf.getHadoopHome() + "/etc/hadoop/core-site.xml");

        dataset1 = conf.get("DATASET1");
        dataset2 = conf.get("DATASET2");

        dataset1_query = new Query(conf.get("DATASET1_QUERY"));
        dataset2_query = new Query(conf.get("DATASET2_QUERY"));

        //System.out.println("INFO Dataset|Query: " + dataset1 + " " + dataset1_query);
        //System.out.println("INFO Dataset|Query: " + dataset2 + " " + dataset2_query);

        dataset1_cutpoints = RangePartitionerUtils.getIntCutPoints(conf.get("DATASET1_CUTPOINTS"));
        dataset2_cutpoints = RangePartitionerUtils.getIntCutPoints(conf.get("DATASET2_CUTPOINTS"));

        dataset1_MDIndex = dataset1_cutpoints == null;
        dataset2_MDIndex = dataset2_cutpoints == null;

        String workingDir = queryConf.getWorkingDir();
        //System.out.println("INFO working dir: " + workingDir);


        AccessMethod dataset1_am = new AccessMethod();
        AccessMethod dataset2_am = new AccessMethod();

        // if there are no cutpoints, we read ranges from MDIndex.

        dataset1_hpinput = new HPJoinInput(dataset1_MDIndex);
        dataset2_hpinput = new HPJoinInput(dataset2_MDIndex);


        String input_dir = workingDir + "/" + dataset1 + "/data";

        // dataset1 is always from MDIndex

        // init RobustTree
        rt1 = initRobustTree(queryConf.getWorkingDir() + "/" + dataset1);

        queryConf.setQuery(dataset1_query);
        dataset1_am.init(queryConf);
        dataset1_hpinput.initialize(listStatus(input_dir), dataset1_am);

        // may update RobustTree on disk

        dataset1_splits = dataset1_hpinput.getIndexScan(queryConf.getJustAccess(), queryConf.getQuery());

        dataset1_splits = resizeSplits(dataset1_splits, dataset1_hpinput.getPartitionIdSizeMap(),
                queryConf.getMaxSplitSize(), queryConf.getMinSplitSize());


        dataset1_int_splits = getIntPartitions(dataset1_splits);

        // dataset2 may be from MDIndex or intermediate join result

        input_dir = workingDir + "/" + dataset2 + "/data";

        if (dataset2_MDIndex) {
            // init RobustTree
            rt2 = initRobustTree(queryConf.getWorkingDir() + "/" + dataset2);
            queryConf.setQuery(dataset2_query);
            dataset2_am.init(queryConf);
            dataset2_hpinput.initialize(listStatus(input_dir), dataset2_am);
            dataset2_int_splits = expandPartitionSplit(dataset2_am.getPartitionSplits(dataset2_query, true));
        } else {
            dataset2_hpinput.initialize(listStatus(input_dir));
            dataset2_int_splits = RangePartitionerUtils.getSplits(dataset2_cutpoints);
        }

        init_bucketInfo(conf, dataset1_int_splits, dataset2_int_splits);

        printStatistics();

        hyperJoinSplit = new ArrayList<PartitionSplit>();
        shuffleJoinSplit =  new ArrayList<PartitionSplit>();

        for (PartitionSplit split : dataset1_splits) {

            int[] split_ids = split.getPartitions();
            int[] dep_split_ids = getOverlappedSplits(split_ids);


            if(dep_split_ids.length > threshold){ // shuffleJoin
                shuffleJoinSplit.add(split);
            } else { // hyperJoin
                hyperJoinSplit.add(split);
            }
        }

        System.out.println("done with JoinPlanner constructor");
    }

    public String getHyperJoinInput(){
        // iter_type, id1:len1:id2:len2:... , id1:len1:id2:len2:... ;...
        StringBuilder sb = new StringBuilder();

        for(PartitionSplit split: hyperJoinSplit){

            if(sb.length() > 0){
                sb.append(";");
            }

            PartitionIterator iter = split.getIterator();
            if(iter instanceof PostFilterIterator){
                sb.append(1 + ",");
            } else {
                sb.append(2 + ",");
            }

            int[] bucketIds = split.getPartitions();
            int[] dep_bucketIDs = getOverlappedSplits(bucketIds);

            long[] bucket_lens = getLengths(1, bucketIds);
            long[] dep_bucket_lens = getLengths(2, dep_bucketIDs);

            for(int i = 0; i < bucketIds.length; i ++){
                if(i > 0){
                    sb.append(":");
                }
                sb.append(bucketIds[i] + ":" + bucket_lens[i]);
            }
            sb.append(",");

            for(int i = 0; i < dep_bucketIDs.length; i ++){
                if(i > 0){
                    sb.append(":");
                }
                sb.append(dep_bucketIDs[i] + ":" + dep_bucket_lens[i]);
            }
        }

        return sb.toString();
    }

    public String getShuffleJoinInput1(){
        // iter,1:100,2:120; ...

        StringBuilder sb = new StringBuilder();

        for(PartitionSplit split: shuffleJoinSplit){
            if(sb.length() > 0){
                sb.append(";");
            }

            PartitionIterator iter = split.getIterator();
            if(iter instanceof PostFilterIterator){
                sb.append(1 + ",");
            } else {
                sb.append(2 + ",");
            }

            int[] bucketIds = split.getPartitions();

            long[] bucket_lens = getLengths(1, bucketIds);

            for(int i = 0; i < bucketIds.length; i ++){
                if(i > 0){
                    sb.append(",");
                }
                sb.append(bucketIds[i] + ":" + bucket_lens[i]);
            }
        }

        return sb.toString();
    }

    public String getShuffleJoinInput2(){
        //1:100;2:120
        HashSet<Integer> bucketId_set = new HashSet<Integer>();

        for(PartitionSplit split: shuffleJoinSplit){
            int[] bucketIds = split.getPartitions();
            int[] dep_bucketIds = getOverlappedSplits(bucketIds);
            for(int i = 0 ;i < dep_bucketIds.length; i ++){
                bucketId_set.add(dep_bucketIds[i]);
            }
        }

        int[] bucketIds = new int[bucketId_set.size()];
        int it = 0;
        for(int i : bucketId_set){
            bucketIds[it++] = i;
        }
        long[] lens = getLengths(2, bucketIds);

        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < bucketIds.length; i ++){
            if(sb.length() > 0){
                sb.append(";");
            }
            sb.append(bucketIds[i] + ":" + lens[i]);
        }

        return sb.toString();
    }

    public Query getDataset1_query(){
        return dataset1_query;
    }

    public Query getDataset2_query(){
        return dataset2_query;
    }

    private Path[] getPaths(int dataset_id, int[] split){
        if(dataset_id == 1){
            return dataset1_hpinput.getPaths(split);
        } else {
            return dataset2_hpinput.getPaths(split);
        }
    }

    private long[] getLengths(int dataset_id, int[] split){
        if(dataset_id == 1){
            return dataset1_hpinput.getLengths(split);
        } else {
            return dataset2_hpinput.getLengths(split);
        }
    }

    private int[] getOverlappedSplits(int[] split){

        HashSet<Integer> overlappedSplits = new HashSet<Integer>();
        for(int i = 0 ;i < split.length; i ++){
            int id = split[i];
            if(overlap_chunks.containsKey(id)){
                overlappedSplits.addAll(overlap_chunks.get(id));
            }
        }

        int[] final_split = new int[overlappedSplits.size()];
        int it = 0;
        for(int i : overlappedSplits){
            final_split[it++] = i;
        }

        return final_split;
    }

    private void printStatistics(){
        System.out.println("Input1: " + dataset1_int_splits.length + " Input2: " + dataset2_int_splits.length);

        int[] hist = new int[16];
        for (int chunk : overlap_chunks.keySet()) {
            ArrayList<Integer> dep_chunk = overlap_chunks.get(chunk);
            int i = 0;
            while ((1 << i) < dep_chunk.size()) {
                i++;
            }
            hist[i]++;
        }

        System.out.println("Histogram:");
        for (int i = 0; i < 16; i++) {
            System.out.println((1 << i) + ": " + hist[i]);
        }



        int sum = 0;

        for(int i = 0 ;i < dataset1_splits.length; i ++){
            PartitionSplit split = dataset1_splits[i];
            int[] chunks = split.getPartitions();
            HashSet<Integer> set = new HashSet<Integer>();
            for(int j = 0; j < chunks.length; j ++){
                for(int k :  overlap_chunks.get(chunks[j])){
                    set.add(k);
                }
            }
            sum += set.size();
        }

        System.out.println(sum + " chunks from table 2 are read!");

    }

    private RobustTree initRobustTree(String path){
        String index = path + "/index";
        String sample = path + "/sample";
        FileSystem fs = HDFSUtils.getFS(queryConf.getHadoopHome()
                + "/etc/hadoop/core-site.xml");

        byte[] indexBytes = HDFSUtils.readFile(fs, index);

        RobustTree rt = new RobustTree();
        rt.unmarshall(indexBytes);

        //byte[] sampleBytes = HDFSUtils.readFile(fs, sample);
        //rt.loadSample(sampleBytes);

        return rt;
    }
    private List<FileStatus> listStatus(String path) {
        ArrayList<FileStatus> input = new ArrayList<FileStatus>();
        FileStatus[] input_array = null;
        try {
            input_array = fs.listStatus(new Path(path));
            for (int i = 0; i < input_array.length; i++) {
                input.add(input_array[i]);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return input;
    }

    private int[] expandPartitionSplit(PartitionSplit[] splits) {
        int totalLength = 0;
        for (int i = 0; i < splits.length; i++) {
            totalLength += splits[i].getPartitions().length;
        }
        int[] partitions = new int[totalLength];
        for (int i = 0, k = 0; i < splits.length; i++) {
            int[] par = splits[i].getPartitions();
            for (int j = 0; j < par.length; j++) {
                partitions[k++] = par[j];
            }
        }
        return partitions;
    }

    private void read_index(Map<Integer, MDIndex.BucketInfo> info, int tree_id, int attr) {


        // If a particular bucket is not in the following map, then the range is (-oo,+oo).

        RobustTree rt;

        if(tree_id == 1){
            rt = rt1;
        } else{
            rt = rt2;
        }

        Map<Integer, MDIndex.BucketInfo> bucketRanges = rt.getBucketRanges(attr);

        int[] buckets = rt.getAllBuckets();

        for (int i = 0; i < buckets.length; i++) {
            int bucket_id = buckets[i];
            if (bucketRanges.containsKey(bucket_id) == false) {
                // hard code, the join key can only be int.
                info.put(bucket_id, new MDIndex.BucketInfo(TypeUtils.TYPE.INT, null, null));
            } else {
                info.put(bucket_id, bucketRanges.get(bucket_id));
            }
        }
    }

    private void read_range(Map<Integer, MDIndex.BucketInfo> info, int[] cutpoints) {
        // [10,200] ~ (-oo, 10] (10, 200] (200, +oo)

        if (cutpoints.length == 0) {
            info.put(0, new MDIndex.BucketInfo(TypeUtils.TYPE.INT, null, null));
        } else {
            info.put(0, new MDIndex.BucketInfo(TypeUtils.TYPE.INT, null, cutpoints[0]));
            for (int i = 1; i < cutpoints.length; i++) {
                info.put(i, new MDIndex.BucketInfo(TypeUtils.TYPE.INT, cutpoints[i - 1] + 1, cutpoints[i]));
            }
            info.put(cutpoints.length, new MDIndex.BucketInfo(TypeUtils.TYPE.INT, cutpoints[cutpoints.length - 1], null));
        }
    }


    private void init_bucketInfo(Configuration conf, int[] dataset1_splits, int[] dataset2_splits) {
        dataset1_bucketInfo = new HashMap<Integer, MDIndex.BucketInfo>();
        dataset2_bucketInfo = new HashMap<Integer, MDIndex.BucketInfo>();

        overlap_chunks = new HashMap<Integer, ArrayList<Integer>>();

        System.out.println(conf.get("JOIN_ATTR1") + " $$$ " + conf.get("JOIN_ATTR2"));

        int join_attr1 = Integer.parseInt(conf.get("JOIN_ATTR1"));
        int join_attr2 = Integer.parseInt(conf.get("JOIN_ATTR2"));

        System.out.println("Populate dataset1_bucketInfo");

        if (dataset1_MDIndex) {
            read_index(dataset1_bucketInfo, 1, join_attr1);
        } else {
            read_range(dataset1_bucketInfo, dataset1_cutpoints);
        }

        //Globals.schema =  Schema.createSchema(dataset2_schema);

        System.out.println("Populate dataset2_bucketInfo");

        if (dataset2_MDIndex) {
            read_index(dataset2_bucketInfo, 2, join_attr2);
        } else {
            read_range(dataset2_bucketInfo, dataset2_cutpoints);
        }

        // filtering

        HashSet<Integer> splits1 = new HashSet<Integer>();
        for (int i = 0; i < dataset1_splits.length; i++) {
            splits1.add(dataset1_splits[i]);
        }

        HashSet<Integer> splits2 = new HashSet<Integer>();
        for (int i = 0; i < dataset2_splits.length; i++) {
            splits2.add(dataset2_splits[i]);
        }

        System.out.println("from data 1");
        for (int i : dataset1_bucketInfo.keySet()) {
            System.out.println(i + " " + dataset1_bucketInfo.get(i));
        }

        System.out.println("from data 2");
        for (int i : dataset2_bucketInfo.keySet()) {
            System.out.println(i + " " + dataset2_bucketInfo.get(i));
        }


        for (Integer i : dataset1_bucketInfo.keySet()) {
            if (splits1.contains(i) == false) continue;

            MDIndex.BucketInfo info_i = dataset1_bucketInfo.get(i);

            for (Integer j : dataset2_bucketInfo.keySet()) {
                if (splits2.contains(j) == false) continue;

                MDIndex.BucketInfo info_j = dataset2_bucketInfo.get(j);

                //System.out.println(i + " from " + dataset1 + " intersects with " + j +  " from "+  dataset2 + " result: " + info_i.overlap(info_j));

                if (info_i.overlap(info_j)) {
                    if (overlap_chunks.containsKey(i) == false) {
                        overlap_chunks.put(i, new ArrayList<Integer>());
                    }
                    overlap_chunks.get(i).add(j);
                }
            }
        }

    }

    private int[] getIntPartitions(PartitionSplit[] splits){
        HashSet<Integer> ids = new HashSet<Integer>();

        for(int i = 0 ;i < splits.length; i ++){
            int[] sub_ids = splits[i].getPartitions();
            for(int j = 0 ;j  < sub_ids.length ; j ++){
                ids.add(sub_ids[j]);
            }
        }
        int[] ret_ids = new int[ids.size()];
        int it = 0;
        for(int id : ids){
            ret_ids[it++] = id;
        }
        return ret_ids;
    }

    /* the following code is stolen from SparkInputFormt

    /**
     * The goal of this method is to check the size of each and break large
     * splits into multiple smaller splits.
     *
     * The maximum split size is read from the configuration and it depends on
     * the size of each machine.
     *
     * @param initialSplits
     * @return
     */
    public PartitionSplit[] resizeSplits(PartitionSplit[] initialSplits,
                                         Map<Integer, Long> partitionSizes, long maxSplitSize,
                                         long minSplitSize) {
        List<PartitionSplit> resizedSplits = Lists.newArrayList();
        ArrayListMultimap<String, PartitionSplit> smallSplits = ArrayListMultimap
                .create();

        // For statistics count the size of data per iterator.
        Map<String, Long> accessSizes = new HashMap<String, Long>();

        for (PartitionSplit split : initialSplits) {
            long splitSize = getPartitionSplitSize(split, partitionSizes);

            // Add to statistics.
            String iterName = split.getIterator().getClass().getName();
            Long existingCount = accessSizes.containsKey(iterName) ?
                    accessSizes.get(iterName) : (long)0;
            accessSizes.put(iterName, existingCount + splitSize);

            if (splitSize > maxSplitSize) {
                // create smaller splits
                resizedSplits.addAll(createSmaller(split, partitionSizes,
                        maxSplitSize));
            } else if (splitSize < minSplitSize) {
                // create larger splits
                smallSplits.put(split.getIterator().getClass().getName(), split);
            } else {
                // just accept as it is
                PartitionIterator itr = split.getIterator();
                resizedSplits.add(new PartitionSplit(split.getPartitions(), itr));
            }
        }

        // Print statistics.
        System.out.println("INFO: Access Sizes");
        for (Map.Entry<String, Long> entry : accessSizes.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }

        for (String key : smallSplits.keySet())
            resizedSplits.addAll(createLarger(smallSplits.get(key),
                    partitionSizes, maxSplitSize));

        return resizedSplits.toArray(new PartitionSplit[resizedSplits.size()]);
    }

    private List<PartitionSplit> createLarger(List<PartitionSplit> splits,
                                              Map<Integer, Long> partitionSizes, long maxSplitSize) {
        List<PartitionSplit> largerSplits = Lists.newArrayList();

        Multimap<Integer, Integer> largerSplitPartitionIds = ArrayListMultimap
                .create();
        long currentSize = 0;
        int largerSplitId = 0;

        for (PartitionSplit split : splits) {
            for (Integer p : split.getPartitions()) {
                long pSize = partitionSizes.containsKey(p) ? partitionSizes
                        .get(p) : 0;
                if (currentSize + pSize > maxSplitSize) {
                    largerSplitId++;
                    currentSize = 0;
                }
                currentSize += pSize;
                largerSplitPartitionIds.put(largerSplitId, p);
            }
        }

        for (Integer k : largerSplitPartitionIds.keySet()) {
            PartitionIterator itr = splits.get(0).getIterator();
            largerSplits.add(new PartitionSplit(Ints
                    .toArray(largerSplitPartitionIds.get(k)), itr));
        }

        return largerSplits;
    }

    private List<PartitionSplit> createSmaller(PartitionSplit split,
                                               Map<Integer, Long> partitionSizes, long maxSplitSize) {
        List<PartitionSplit> smallerSplits = Lists.newArrayList();

        int[] partitions = split.getPartitions();
        long currentSize = 0;
        Multimap<Integer, Integer> splitPartitionIds = ArrayListMultimap
                .create();
        int splitId = 0;

        for (int i = 0; i < partitions.length; i++) {
            long pSize = partitionSizes.containsKey(partitions[i]) ? partitionSizes
                    .get(partitions[i]) : 0;
            if (currentSize + pSize > maxSplitSize) {
                splitId++;
                currentSize = 0;
            }
            currentSize += pSize;
            splitPartitionIds.put(splitId, partitions[i]);
        }

        for (Integer k : splitPartitionIds.keySet()) {
            PartitionIterator itr = split.getIterator();
            smallerSplits.add(new PartitionSplit(Ints.toArray(splitPartitionIds
                    .get(k)), itr));
        }

        return smallerSplits;
    }

    private long getPartitionSplitSize(PartitionSplit split,
                                       Map<Integer, Long> partitionIdSizeMap) {
        if (partitionIdSizeMap == null) {
            System.err.println("partition size map is null");
            System.exit(0);
        }
        long size = 0;
        for (int pid : split.getPartitions()) {
            if (partitionIdSizeMap.containsKey(pid))
                size += partitionIdSizeMap.get(pid);
            else
                System.err.println("partitoion size not found: " + pid);
        }
        return size;
    }
}
