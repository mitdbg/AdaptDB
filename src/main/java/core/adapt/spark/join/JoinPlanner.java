package core.adapt.spark.join;


import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.primitives.Ints;

import core.adapt.JoinQuery;


import core.adapt.iterator.JoinRepartitionIterator;
import core.adapt.iterator.PartitionIterator;
import core.adapt.iterator.PostFilterIterator;
import core.common.index.JoinRobustTree;
import core.common.index.MDIndex;

import core.utils.HDFSUtils;
import core.utils.RangePartitionerUtils;
import core.utils.TypeUtils;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import core.adapt.AccessMethod.PartitionSplit;


import java.io.File;
import java.io.IOException;

import java.util.*;

/**
 * Created by ylu on 1/5/16.
 */
public class JoinPlanner {
    FileSystem fs;

    private SparkJoinQueryConf queryConf;
    private String dataset1, dataset2;
    private JoinQuery dataset1_query, dataset2_query;
    private int[] dataset1_cutpoints, dataset2_cutpoints;
    private boolean dataset1_MDIndex, dataset2_MDIndex;

    private Map<Integer, MDIndex.BucketInfo> dataset1_bucketInfo;
    private Map<Integer, MDIndex.BucketInfo> dataset2_bucketInfo;
    private PartitionSplit[] dataset1_splits, dataset2_splits;
    private int[] dataset1_int_splits, dataset2_int_splits;

    private Map<Integer, ArrayList<Integer>> overlap_chunks;
    private HPJoinInput dataset1_hpinput, dataset2_hpinput;

    // 1 for PostFilterIterator, 2 for JoinRepartitionIterator
    private Map<Integer, Integer> iteratorType;


    private ArrayList<PartitionSplit> hyperJoinSplit, shuffleJoinSplit;

    private int threshold = 8;


    public JoinPlanner(Configuration conf) {

        System.out.println("INFO: in JoinPlanner constructor");

        queryConf = new SparkJoinQueryConf(conf);

        fs = HDFSUtils.getFSByHadoopHome(queryConf.getHadoopHome());

        dataset1 = conf.get("DATASET1");
        dataset2 = conf.get("DATASET2");

        dataset1_query = new JoinQuery(conf.get("DATASET1_QUERY"));
        dataset2_query = new JoinQuery(conf.get("DATASET2_QUERY"));

        dataset1_cutpoints = RangePartitionerUtils.getIntCutPoints(conf.get("DATASET1_CUTPOINTS"));
        dataset2_cutpoints = RangePartitionerUtils.getIntCutPoints(conf.get("DATASET2_CUTPOINTS"));

        dataset1_MDIndex = dataset1_cutpoints == null;
        dataset2_MDIndex = dataset2_cutpoints == null;

        hyperJoinSplit = new ArrayList<PartitionSplit>();
        shuffleJoinSplit = new ArrayList<PartitionSplit>();

        String workingDir = queryConf.getWorkingDir();
        //System.out.println("INFO working dir: " + workingDir);

        JoinAccessMethod dataset1_am = new JoinAccessMethod();
        queryConf.setJoinQuery(dataset1_query);
        if (dataset1_MDIndex) {
            dataset1_am.init(queryConf);
            dataset1_int_splits = expandPartitionSplit(dataset1_am.getPartitionSplits(dataset1_query, true));

        } else {
            dataset1_int_splits = RangePartitionerUtils.getSplits(dataset1_cutpoints);
        }
        dataset1_hpinput = new HPJoinInput(dataset1_MDIndex);
        String input_dir = workingDir + "/" + dataset1 + "/data";
        dataset1_hpinput.initialize(listStatus(input_dir), dataset1_am);


        JoinAccessMethod dataset2_am = new JoinAccessMethod();
        queryConf.setJoinQuery(dataset2_query);

        if (dataset2_MDIndex) {
            dataset2_am.init(queryConf);
            dataset2_int_splits = expandPartitionSplit(dataset2_am.getPartitionSplits(dataset2_query, true));
        } else {
            dataset2_int_splits = RangePartitionerUtils.getSplits(dataset2_cutpoints);
        }
        dataset2_hpinput = new HPJoinInput(dataset2_MDIndex);
        input_dir = workingDir + "/" + dataset2 + "/data";
        dataset2_hpinput.initialize(listStatus(input_dir), dataset2_am);

        init_bucketInfo(conf, dataset1_int_splits, dataset1_am, dataset2_int_splits, dataset2_am);

        // optimize for the JoinRobustTree

        System.out.println("Optimizing dataset 1");

        if (dataset1_MDIndex) {
            dataset1_splits = dataset1_hpinput.getIndexScan(queryConf.getJustAccess(), dataset1_query);
        } else {
            dataset1_splits = getPartitionSplits(dataset1_int_splits, dataset1_query);
        }

        System.out.println("Optimizing dataset 2");

        if (dataset2_MDIndex) {
            dataset2_splits = dataset2_hpinput.getIndexScan(queryConf.getJustAccess(), dataset2_query);
        } else {
            dataset2_splits = getPartitionSplits(dataset2_int_splits, dataset2_query);
        }

        System.out.println("init iteratorType");

        init_iteratorType(dataset2_splits);

        System.out.println("print stats");

        printStatistics();

        extractJoin(dataset1_splits, dataset1_hpinput.getPartitionIdSizeMap(), queryConf.getMaxSplitSize());

        System.out.println("done with JoinPlanner constructor");
    }

    private void extractJoin(PartitionSplit[] splits, Map<Integer, Long> partitionSizes, long maxSplitSize){
        // TODO: the threshold should be on the number of blocks on LHS, not RHS, since we are building hashtable on LHS

        HashMap<Integer, Integer> counters = new HashMap<Integer, Integer>();

        for(int i = 0 ;i < dataset1_splits.length; i ++){
            PartitionSplit split = splits[i];
            int[] bids = split.getPartitions();

            for(int j = 0; j < bids.length; j ++){
                ArrayList<Integer> dep_bids = overlap_chunks.get(bids[j]);
                for(int dep_id : dep_bids){
                    if(counters.containsKey(dep_id) == false){
                        counters.put(dep_id, 0);
                    }
                    counters.put(dep_id, counters.get(dep_id) + 1);
                }
            }
        }

        // filter out low counts

        HashSet<Integer> shuffleJoinBids = new HashSet<Integer>();

        for(int bid:  counters.keySet()){
            if(counters.get(bid) > threshold){
                shuffleJoinBids.add(bid);
            }
        }

        for(int i = 0 ;i < dataset1_splits.length; i ++){
            PartitionSplit split = splits[i];
            int[] bids = split.getPartitions();

            ArrayList<Integer> shuffle_ids = new ArrayList<Integer>();
            ArrayList<Integer> hyper_ids = new ArrayList<Integer>();

            for(int j = 0; j < bids.length; j ++){
                boolean shuffle = false;
                ArrayList<Integer> dep_bids = overlap_chunks.get(bids[j]);
                for(int k = 0 ; k < dep_bids.size(); k ++){
                    if(shuffleJoinBids.contains(dep_bids.get(k))){
                        shuffle = true;
                        break;
                    }
                }
                if(shuffle){
                    shuffle_ids.add(bids[j]);
                } else {
                    hyper_ids.add(bids[j]);
                }
            }

            if(shuffle_ids.size() > 0){
                int[] shuffle_ids_int = new int[shuffle_ids.size()];
                for(int j = 0 ; j< shuffle_ids_int.length; j ++){
                    shuffle_ids_int[j] = shuffle_ids.get(j);
                }
                PartitionSplit shuffle_split = new PartitionSplit(shuffle_ids_int, split.getIterator());
                shuffleJoinSplit.add(shuffle_split);
            }
            if(hyper_ids.size() > 0){
                int[] hyper_ids_int = new int[hyper_ids.size()];
                for(int j = 0 ; j< hyper_ids_int.length; j ++){
                    hyper_ids_int[j] = hyper_ids.get(j);
                }
                ArrayList<PartitionSplit> hyper_splits = resizeSplits(split.getIterator(),hyper_ids_int,partitionSizes, maxSplitSize);
                for(PartitionSplit hs : hyper_splits){
                    hyperJoinSplit.add(hs);
                }
            }
        }
    }


    private PartitionSplit[] getPartitionSplits(int[] bids, JoinQuery q) {
        PostFilterIterator pi = new PostFilterIterator(q.castToQuery());
        PartitionSplit psplit = new PartitionSplit(bids, pi);
        PartitionSplit[] ps = new PartitionSplit[1];
        ps[0] = psplit;
        return ps;
    }

    private void init_iteratorType(PartitionSplit[] splits) {
        // 1 for PostFilterIterator, 2 for JoinRepartitionIterator
        System.out.println("Bucket Iterator type: ");
        iteratorType = new HashMap<Integer, Integer>();
        for (int i = 0; i < splits.length; i++) {
            int[] bids = splits[i].getPartitions();
            PartitionIterator it = splits[i].getIterator();
            int type = 0;
            if (it instanceof PostFilterIterator) {
                type = 1;
            } else {
                type = 2;
            }
            for (int j = 0; j < bids.length; j++) {
                //System.out.println("bucket: " + bids[j] + " type: " + type);
                iteratorType.put(bids[j], type);
            }
        }
    }

    public String getHyperJoinInput() {
        // iter1_type, id1:len1:id2:len2:... , id1:len1:iter2_type:id2:len2:iter2_type... ;...
        StringBuilder sb = new StringBuilder();

        for (PartitionSplit split : hyperJoinSplit) {

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
            int[] dep_bucketIDs = getOverlappedSplits(bucketIds);

            long[] bucket_lens = getLengths(1, bucketIds);
            long[] dep_bucket_lens = getLengths(2, dep_bucketIDs);

            for (int i = 0; i < bucketIds.length; i++) {
                if (i > 0) {
                    sb.append(":");
                }
                sb.append(bucketIds[i] + ":" + bucket_lens[i]);
            }
            sb.append(",");

            for (int i = 0; i < dep_bucketIDs.length; i++) {
                if (i > 0) {
                    sb.append(":");
                }
                int type = iteratorType.get(dep_bucketIDs[i]);
                sb.append(dep_bucketIDs[i] + ":" + dep_bucket_lens[i] + ":" + type);
                iteratorType.put(dep_bucketIDs[i], 1);// we can only use JoinRepartitionIterator once.
            }
        }

        return sb.toString();
    }

    public String getShuffleJoinInput1() {
        // iter,1:100,2:120; ...

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

            long[] bucket_lens = getLengths(1, bucketIds);

            for (int i = 0; i < bucketIds.length; i++) {
                if (i > 0) {
                    sb.append(",");
                }
                sb.append(bucketIds[i] + ":" + bucket_lens[i]);
            }
        }

        return sb.toString();
    }

    public String getShuffleJoinInput2() {
        //1:100:type;2:120:type
        HashSet<Integer> bucketId_set = new HashSet<Integer>();

        for (PartitionSplit split : shuffleJoinSplit) {
            int[] bucketIds = split.getPartitions();
            int[] dep_bucketIds = getOverlappedSplits(bucketIds);
            for (int i = 0; i < dep_bucketIds.length; i++) {
                bucketId_set.add(dep_bucketIds[i]);
            }
        }

        int[] bucketIds = new int[bucketId_set.size()];
        int it = 0;
        for (int i : bucketId_set) {
            bucketIds[it++] = i;
        }
        long[] lens = getLengths(2, bucketIds);

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < bucketIds.length; i++) {
            if (sb.length() > 0) {
                sb.append(";");
            }
            sb.append(bucketIds[i] + ":" + lens[i] + ":" + iteratorType.get(bucketIds[i]));
        }

        return sb.toString();
    }

    public JoinQuery getDataset1_query() {
        return dataset1_query;
    }

    public JoinQuery getDataset2_query() {
        return dataset2_query;
    }

    private Path[] getPaths(int dataset_id, int[] split) {
        if (dataset_id == 1) {
            return dataset1_hpinput.getPaths(split);
        } else {
            return dataset2_hpinput.getPaths(split);
        }
    }

    private long[] getLengths(int dataset_id, int[] split) {
        if (dataset_id == 1) {
            return dataset1_hpinput.getLengths(split);
        } else {
            return dataset2_hpinput.getLengths(split);
        }
    }

    private int[] getOverlappedSplits(int[] split) {

        HashSet<Integer> overlappedSplits = new HashSet<Integer>();
        for (int i = 0; i < split.length; i++) {
            int id = split[i];
            if (overlap_chunks.containsKey(id)) {
                overlappedSplits.addAll(overlap_chunks.get(id));
            }
        }

        int[] final_split = new int[overlappedSplits.size()];
        int it = 0;
        for (int i : overlappedSplits) {
            final_split[it++] = i;
        }

        return final_split;
    }

    private void printStatistics() {
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

        for (int i = 0; i < dataset1_splits.length; i++) {
            PartitionSplit split = dataset1_splits[i];
            int[] chunks = split.getPartitions();
            HashSet<Integer> set = new HashSet<Integer>();
            for (int j = 0; j < chunks.length; j++) {
                for (int k : overlap_chunks.get(chunks[j])) {
                    set.add(k);
                }
            }
            sum += set.size();
        }

        System.out.println(sum + " chunks from table 2 are read!");

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

    private void read_index(Map<Integer, MDIndex.BucketInfo> info, JoinRobustTree rt, int attr) {
        // If a particular bucket is not in the following map, then the range is (-oo,+oo).

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


    private void init_bucketInfo(Configuration conf, int[] dataset1_splits, JoinAccessMethod jam1, int[] dataset2_splits, JoinAccessMethod jam2) {
        dataset1_bucketInfo = new HashMap<Integer, MDIndex.BucketInfo>();
        dataset2_bucketInfo = new HashMap<Integer, MDIndex.BucketInfo>();

        overlap_chunks = new HashMap<Integer, ArrayList<Integer>>();


        System.out.println("Populate dataset1_bucketInfo");

        if (dataset1_MDIndex) {
            read_index(dataset1_bucketInfo, jam1.opt.getIndex(), dataset1_query.getJoinAttribute());
        } else {
            read_range(dataset1_bucketInfo, dataset1_cutpoints);
        }

        //Globals.schema =  Schema.createSchema(dataset2_schema);

        System.out.println("Populate dataset2_bucketInfo");

        if (dataset2_MDIndex) {
            read_index(dataset2_bucketInfo, jam2.opt.getIndex(), dataset2_query.getJoinAttribute());
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

    private int[] getIntPartitions(PartitionSplit[] splits) {
        HashSet<Integer> ids = new HashSet<Integer>();

        for (int i = 0; i < splits.length; i++) {
            int[] sub_ids = splits[i].getPartitions();
            for (int j = 0; j < sub_ids.length; j++) {
                ids.add(sub_ids[j]);
            }
        }
        int[] ret_ids = new int[ids.size()];
        int it = 0;
        for (int id : ids) {
            ret_ids[it++] = id;
        }
        return ret_ids;
    }

    private int getIntersectionSize(HashSet<Integer> chunks, ArrayList<Integer> vals) {
        int sum = 0;
        for (int i = 0; i < vals.size(); i++) {
            if (chunks.contains(vals.get(i))) {
                sum++;
            }
        }
        return sum;
    }

    /* the following code uses heuristic grouping
       TODO: add different grouping algos
    */

    private ArrayList<PartitionSplit> resizeSplits(PartitionIterator partitionIter, int[] bids, Map<Integer, Long> partitionSizes, long maxSplitSize) {
        ArrayList<PartitionSplit> resizedSplits = new ArrayList<PartitionSplit>();

        Random rand = new Random();
        rand.setSeed(0); // Making things more deterministic.

        LinkedList<Integer> buckets = new LinkedList<Integer>();

        int size = bids.length;
        for (int i = 0; i < bids.length; i++) {
            buckets.add(bids[i]);
        }

        while (size > 0) {
            ArrayList<Integer> cur_split = new ArrayList<Integer>();
            HashSet<Integer> chunks = new HashSet<Integer>();
            long splitAvailableSize = maxSplitSize, totalSize = 0;
            while (size > 0 && splitAvailableSize > 0) {
                int maxIntersection = -1;
                int best_offset = -1;

                ListIterator<Integer> it = buckets.listIterator();
                int offset = 0;

                while (it.hasNext()) {
                    int value = it.next();
                    if (maxIntersection == -1) {
                        maxIntersection = getIntersectionSize(chunks, overlap_chunks.get(value));
                        best_offset = offset;
                    } else {
                        int curIntersection = getIntersectionSize(chunks, overlap_chunks.get(value));
                        if (curIntersection > maxIntersection) {
                            maxIntersection = curIntersection;
                            best_offset = offset;
                        }
                    }
                    offset++;
                }
                int bucket = buckets.get(best_offset);
                splitAvailableSize -= partitionSizes.get(bucket);

                if (splitAvailableSize >= 0) {
                    totalSize += partitionSizes.get(bucket);
                    cur_split.add(bucket);
                    for (int rhs : overlap_chunks.get(bucket)) {
                        chunks.add(rhs);
                    }
                    buckets.remove(best_offset);
                    size--;
                }
            }

            int[] split_bids = new int[cur_split.size()];
            for (int i = 0; i < split_bids.length; i++) {
                split_bids[i] = cur_split.get(i);
            }

            PartitionSplit split = new PartitionSplit(split_bids, partitionIter);
            resizedSplits.add(split);
            //System.out.println("split size: " + totalSize + " " + Arrays.toString(split_bids));
        }
        return resizedSplits;
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
