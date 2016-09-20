package perf.benchmark;

import core.common.globals.Globals;
import core.common.globals.TableInfo;
import core.common.index.JoinRobustTree;
import core.common.index.MDIndex;
import core.common.key.ParsedTupleList;
import core.utils.ConfUtils;
import core.utils.HDFSUtils;
import core.utils.TypeUtils;
import org.apache.hadoop.fs.FileSystem;

import java.util.*;

/**
 * Created by ylu on 1/21/16.
 */

public class JoinAttrLevel {

    public static String lineitem = "lineitem";
    public static String orders = "orders";
    public static String customer = "customer";

    public static int lineitemBuckets = 16384, ordersBuckets = 2048, customerBuckets = 512;

    public static ParsedTupleList lineitemSample, ordersSample, customerSample;

    public static ParsedTupleList loadSample(String tableName){

        ConfUtils cfg = new ConfUtils("/Users/ylu/Documents/workspace/AdaptDB/conf/local.properties");
        FileSystem fs = HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME());
        // Load table info.
        Globals.loadTableInfo(tableName, cfg.getHDFS_WORKING_DIR(), fs);
        TableInfo tableInfo = Globals.getTableInfo(tableName);

        String pathToSample = cfg.getHDFS_WORKING_DIR()+ "/" + tableInfo.tableName + "/sample";

        byte[] sampleBytes = HDFSUtils.readFile(fs, pathToSample);

        // read all the sample files and put them into the sample key set
        ParsedTupleList sample = new ParsedTupleList(tableInfo.getTypeArray());
        sample.unmarshall(sampleBytes, tableInfo.delimiter);

        return sample;
    }

    public static void init(){
        lineitemSample = loadSample(lineitem);
        ordersSample = loadSample(orders);
        customerSample = loadSample(customer);
    }

    public static JoinRobustTree getAdaptDbIndex(String tableName, ParsedTupleList sample, int numBuckets, int levelsForJoinAttr, int joinAttr){

        TableInfo tableInfo = Globals.getTableInfo(tableName);

        JoinRobustTree rt = new JoinRobustTree(tableInfo);
        rt.joinAttributeDepth = levelsForJoinAttr;
        rt.setMaxBuckets(numBuckets);
        rt.loadSample(sample);
        rt.initProbe(joinAttr);

        byte[] bytes = rt.marshall();
        rt.unmarshall(bytes);

        return rt;
    }

    public static void read_index(Map<Integer, MDIndex.BucketInfo> info, JoinRobustTree rt, int attr) {
        // If a particular bucket is not in the following map, then the range is (-oo,+oo).

        Map<Integer, MDIndex.BucketInfo> bucketRanges = rt.getBucketRanges(attr);

        int[] buckets = rt.getAllBuckets();

        for (int i = 0; i < buckets.length; i++) {
            int bucket_id = buckets[i];
            if (bucketRanges.containsKey(bucket_id) == false) {
                // hard code, the join key can only be int.

                info.put(bucket_id, new MDIndex.BucketInfo(TypeUtils.TYPE.LONG, null, null));
            } else {
                //System.out.println(">>> " + bucket_id + " " + bucketRanges.get(bucket_id));
                info.put(bucket_id, bucketRanges.get(bucket_id));
            }
        }
    }

    public static int getIntersectionSize(HashSet<Integer> setValues, ArrayList<Integer> listValues) {
        int size = 0;
        for (int i = 0; i < listValues.size(); i++) {
            if (setValues.contains(listValues.get(i))) {
                size++;
            }
        }
        return size;
    }



    public static int getCost(JoinRobustTree rt1, JoinRobustTree rt2, int joinAttr1, int joinAttr2, int bufferSize){
        Map<Integer, MDIndex.BucketInfo> dataset1_bucketInfo = new HashMap<Integer, MDIndex.BucketInfo>();
        Map<Integer, MDIndex.BucketInfo> dataset2_bucketInfo = new HashMap<Integer, MDIndex.BucketInfo>();

        Map<Integer, ArrayList<Integer>> overlap_chunks = new HashMap<Integer, ArrayList<Integer>>();

        read_index(dataset1_bucketInfo, rt1, joinAttr1);
        read_index(dataset2_bucketInfo, rt2, joinAttr2);


        for (Integer i : dataset1_bucketInfo.keySet()) {
            MDIndex.BucketInfo info_i = dataset1_bucketInfo.get(i);
            for (Integer j : dataset2_bucketInfo.keySet()) {
                MDIndex.BucketInfo info_j = dataset2_bucketInfo.get(j);
                if (info_i.overlap(info_j)) {
                    if (overlap_chunks.containsKey(i) == false) {
                        overlap_chunks.put(i, new ArrayList<Integer>());
                    }
                    overlap_chunks.get(i).add(j);
                }
            }
        }

        int cost = 0;

        int[] bids = rt1.getAllBuckets();
        int size = bids.length;

        LinkedList<Integer> buckets = new LinkedList<Integer>();

        for (int i = 0; i < bids.length; i++) {
            buckets.add(bids[i]);
        }

        while (size > 0) {
            ArrayList<Integer> cur_split = new ArrayList<Integer>();
            HashSet<Integer> chunks = new HashSet<Integer>();
            int splitAvailableSize = bufferSize;
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

                splitAvailableSize --;


                cur_split.add(bucket);
                for (int rhs : overlap_chunks.get(bucket)) {
                    chunks.add(rhs);
                }
                buckets.remove(best_offset);
                size--;

            }

            cost += chunks.size();
        }

        return cost;
    }


    public static void main(String[] args){

        init();


        // l_orderkey == 0, o_orderkey == 0
        // lineitem 14, orders 11

        // o_custkey == 1, c_custkey == 0
        // orders 11, customer 9

        int bufferSize = 128; // 4G / 32 MB

        for(int i = 0; i <= 14; i ++){
            JoinRobustTree lineitemTree = getAdaptDbIndex(lineitem, lineitemSample, lineitemBuckets, i, 0);

            for(int j = 0; j <= 11; j ++){
                JoinRobustTree ordersTree = getAdaptDbIndex(orders, ordersSample, ordersBuckets, j, 0);
                int cost = getCost(lineitemTree, ordersTree, 0, 0, bufferSize);
                System.out.printf("lineitem %d orders %d cost %d\n", i, j , cost);
            }
        }

        for(int i = 0; i <= 11; i ++){
            JoinRobustTree ordersTree = getAdaptDbIndex(orders, ordersSample, ordersBuckets, i, 1);

            for(int j = 0; j <= 9; j ++){
                JoinRobustTree customerTree = getAdaptDbIndex(customer, customerSample, customerBuckets, j, 0);
                int cost = getCost(ordersTree, customerTree, 1, 0, bufferSize);
                System.out.printf("orders %d customer %d cost %d\n", i, j , cost);
            }
        }

        System.out.println("Done!");
    }
}