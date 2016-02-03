package core.adapt.spark.join;


import core.adapt.AccessMethod;
import core.adapt.AccessMethod.PartitionSplit;
import core.adapt.Query;
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

import java.io.IOException;
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


    private Map<Integer, MDIndex.BucketInfo> dataset1_bucketInfo;
    private Map<Integer, MDIndex.BucketInfo> dataset2_bucketInfo;
    private int[] dataset1_splits, dataset2_splits;

    private Map<Integer, ArrayList<Integer> > overlap_chunks;
    private HPJoinInput dataset1_hpinput, dataset2_hpinput;

    private static int threshold = 2;
    private Map<Integer, ArrayList<Integer> > shuffleJoin;
    private Map<Integer, ArrayList<Integer> > hyperJoin;

    private List<FileStatus> listStatus(String path){
        ArrayList<FileStatus> input = new ArrayList<FileStatus>();
        FileStatus[] input_array = null;
        try {
            input_array = fs.listStatus(new Path(path));
            for(int i= 0 ;i < input_array.length; i ++){
                input.add(input_array[i]);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return input;
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

    private void read_index(Map<Integer, MDIndex.BucketInfo> info, String path, int attr){
        String index = path + "/index";
        String sample = path + "/sample";
        FileSystem fs = HDFSUtils.getFSByHadoopHome(queryConf.getHadoopHome());

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


    private void init_bucketInfo(Configuration conf, int[] dataset1_splits, int[] dataset2_splits){
        dataset1_bucketInfo = new HashMap<Integer, MDIndex.BucketInfo>();
        dataset2_bucketInfo = new HashMap<Integer, MDIndex.BucketInfo>();

        overlap_chunks = new HashMap<Integer, ArrayList<Integer> >();

        System.out.println(conf.get("JOIN_ATTR1") + " $$$ " + conf.get("JOIN_ATTR2"));

        int join_attr1 = Integer.parseInt(conf.get("JOIN_ATTR1"));
        int join_attr2  = Integer.parseInt(conf.get("JOIN_ATTR2"));

        System.out.println("Populate dataset1_bucketInfo");

        if(dataset1_MDIndex){
            read_index(dataset1_bucketInfo, queryConf.getWorkingDir() + "/" + dataset1,join_attr1);
        } else {
            read_range(dataset1_bucketInfo, dataset1_cutpoints);
        }

        //Globals.schema =  Schema.createSchema(dataset2_schema);

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

        System.out.println("from data 1");
        for(int i : dataset1_bucketInfo.keySet()){
            System.out.println(i + " " + dataset1_bucketInfo.get(i));
        }

        System.out.println("from data 2");
        for(int i : dataset2_bucketInfo.keySet()){
            System.out.println(i + " " + dataset2_bucketInfo.get(i));
        }


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

    public String getHyperJoinOverlap(){
        StringBuilder sb = new StringBuilder();
        for(int chunk: hyperJoin.keySet()){
            if(sb.length() > 0 ){
                sb.append(";");
            }
            sb.append(chunk);
            for(int dep_chunk: hyperJoin.get(chunk)){
                sb.append("," + dep_chunk);
            }
        }
        return sb.toString();
    }

    public String getString(int[] chunks, long[] chunks_len){
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < chunks.length; i ++){
            if(sb.length() > 0){
                sb.append(";");
            }
            sb.append(chunks[i] + ":" + chunks_len[i]);
        }
        return sb.toString();
    }

    public String[] getJoin(Map<Integer, ArrayList<Integer> > join){
        int[] chunks = new int[join.keySet().size()];
        HashSet<Integer> hash_dep_chunks = new  HashSet<Integer>();
        int i = 0;
        for(int chunk: join.keySet()){
            chunks[i++] = chunk;
            for(int dep_chunk: join.get(chunk)){
                hash_dep_chunks.add(dep_chunk);
            }
        }
        int[] dep_chunks = new int[hash_dep_chunks.size()];
        i = 0;
        for(int dep_chunk: hash_dep_chunks){
            dep_chunks[i++] = dep_chunk;
        }

        long[] chunks_len = dataset1_hpinput.getLengths(chunks);
        long[] dep_chunks_len =  dataset2_hpinput.getLengths(dep_chunks);

        String[] ret = new String[2];
        ret[0] = getString(chunks, chunks_len);
        ret[1] = getString(dep_chunks, dep_chunks_len);

        return ret;
    }

    // [0] dataset1, [1] dataset2
    public String[] getHyperJoin(){
        return getJoin(hyperJoin);
    }

    // [0] dataset1, [1] dataset2
    public String[] getShuffleJoin(){
        return getJoin(shuffleJoin);
    }

    public JoinPlanner(Configuration conf){

        System.out.println("INFO: in JoinPlanner constructor");

        queryConf = new SparkQueryConf(conf);

        fs = HDFSUtils.getFSByHadoopHome(queryConf.getHadoopHome());

        dataset1 = conf.get("DATASET1");
        dataset2 = conf.get("DATASET2");

        dataset1_query = new Query(conf.get("DATASET1_QUERY"));
        dataset2_query = new Query(conf.get("DATASET2_QUERY"));


        System.out.println("INFO Dataset|Query: " + dataset1 + " " + dataset1_query);
        System.out.println("INFO Dataset|Query: " + dataset2 + " " + dataset2_query);

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


        String input_dir =  workingDir + "/" + dataset1 + "/data";

        System.out.println(input_dir);

        if(dataset1_MDIndex){
            queryConf.setQuery(dataset1_query);
            dataset1_am.init(queryConf);
            dataset1_hpinput.initialize(listStatus(input_dir), dataset1_am);
            dataset1_splits = expandPartitionSplit(dataset1_am.getPartitionSplits(dataset1_query, true));
        } else {
            dataset1_hpinput.initialize(listStatus(input_dir));
            dataset1_splits = RangePartitionerUtils.getSplits(dataset1_cutpoints);
        }


        input_dir =  workingDir + "/" + dataset2 + "/data";
        if(dataset2_MDIndex){
            queryConf.setQuery(dataset2_query);
            dataset2_am.init(queryConf);
            dataset2_hpinput.initialize(listStatus(input_dir), dataset2_am);
            dataset2_splits = expandPartitionSplit(dataset2_am.getPartitionSplits(dataset2_query, true));
        } else {
            dataset2_hpinput.initialize(listStatus(input_dir));
            dataset2_splits = RangePartitionerUtils.getSplits(dataset2_cutpoints);
        }

        //System.out.println("Barrier 3");

        init_bucketInfo(conf, dataset1_splits, dataset2_splits);

        // hard code a strategy

        shuffleJoin = new HashMap<Integer, ArrayList<Integer> >();
        hyperJoin = new HashMap<Integer, ArrayList<Integer> >();

        System.out.println("Dependency: ");

        for(int chunk : overlap_chunks.keySet()){
            ArrayList<Integer> dep_chunk = overlap_chunks.get(chunk);
            if(dep_chunk.size() > threshold) { // shuflleJoin
                shuffleJoin.put(chunk, dep_chunk);
            } else {
                hyperJoin.put(chunk, dep_chunk);
            }
            System.out.print(chunk + ":");
            for(int i :  overlap_chunks.get(chunk)){
                System.out.print(i + ";");
            }
            System.out.println();
        }

    }

}
