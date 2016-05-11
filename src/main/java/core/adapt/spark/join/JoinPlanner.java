package core.adapt.spark.join;


import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.primitives.Ints;

import core.adapt.JoinQuery;


import core.adapt.iterator.JoinRepartitionIterator;
import core.adapt.iterator.PartitionIterator;
import core.adapt.iterator.PostFilterIterator;
import core.common.globals.Globals;
import core.common.globals.TableInfo;
import core.common.index.JoinRobustTree;
import core.common.index.MDIndex;

import core.common.key.ParsedTupleList;
import core.utils.HDFSUtils;
import core.utils.RangePartitionerUtils;
import core.utils.TypeUtils;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import core.adapt.AccessMethod.PartitionSplit;


import java.io.File;
import java.io.IOException;

import java.util.*;

/**
 * Created by ylu on 1/5/16.
 */
public class JoinPlanner {

    private FileSystem fs;
    private SparkJoinQueryConf queryConf;
    private String dataset1, dataset2;
    private JoinQuery dataset1_query, dataset2_query;
    private List<JoinQuery> dataset1_queryWindow, dataset2_queryWindow;
    private TableInfo dataset1_tableInfo, dataset2_tableInfo;

    private Map<Integer, MDIndex.BucketInfo> dataset1_bucketInfo, dataset2_bucketInfo;

    private HPJoinInput dataset1_hpinput, dataset2_hpinput;
    private Map<Integer, JoinAccessMethod> dataset1_am, dataset2_am;
    private Map<Integer, ArrayList<Integer>> dataset1_scan_blocks, dataset2_scan_blocks;
    private Map<Integer, Integer> dataset1_iterator_type, dataset2_iterator_type;

    private Map<Integer, ArrayList<Integer>> overlap_chunks;

    private ArrayList<PartitionSplit> hyperJoinSplit, shuffleJoinSplit1, shuffleJoinSplit2;
    public String hyperjoin, shufflejoin1, shufflejoin2;

    public JoinPlanner(Configuration conf) {

        System.out.println("INFO: in JoinPlanner constructor");

        queryConf = new SparkJoinQueryConf(conf);
        fs = HDFSUtils.getFSByHadoopHome(queryConf.getHadoopHome());

        dataset1 = conf.get("DATASET1");
        dataset2 = conf.get("DATASET2");

        dataset1_query = new JoinQuery(conf.get("DATASET1_QUERY"));
        dataset2_query = new JoinQuery(conf.get("DATASET2_QUERY"));

        dataset1_queryWindow = loadQueries(dataset1, queryConf);
        dataset2_queryWindow = loadQueries(dataset2, queryConf);

        dataset1_queryWindow.add(dataset1_query);
        dataset2_queryWindow.add(dataset2_query);

        persistQueryToDisk(dataset1_query, queryConf);
        persistQueryToDisk(dataset2_query, queryConf);

        dataset1_hpinput = new HPJoinInput();
        dataset2_hpinput = new HPJoinInput();
        dataset1_am = new HashMap<Integer, JoinAccessMethod>();
        dataset2_am = new HashMap<Integer, JoinAccessMethod>();

        dataset1_iterator_type = new HashMap<Integer, Integer>();
        dataset2_iterator_type = new HashMap<Integer, Integer>();

        dataset1_bucketInfo = new HashMap<Integer, MDIndex.BucketInfo>();
        dataset2_bucketInfo = new HashMap<Integer, MDIndex.BucketInfo>();

        overlap_chunks = new HashMap<Integer, ArrayList<Integer>>();

        dataset1_scan_blocks = new HashMap<Integer, ArrayList<Integer>>();
        dataset2_scan_blocks = new HashMap<Integer, ArrayList<Integer>>();

        hyperJoinSplit = new ArrayList<PartitionSplit>();
        shuffleJoinSplit1 = new ArrayList<PartitionSplit>();
        shuffleJoinSplit2 = new ArrayList<PartitionSplit>();

        hyperjoin = "";
        shufflejoin1 = "";
        shufflejoin2 = "";

        // set up table info and get partitions for each table
        setupTableInfo();

        int[] dataset1_partitions = dataset1_tableInfo.partitions;
        int[] dataset2_partitions = dataset1_tableInfo.partitions;

        speculative_repartition(dataset1, dataset1_query, dataset1_queryWindow, dataset1_tableInfo, dataset1_hpinput, dataset1_am, dataset1_scan_blocks, dataset1_iterator_type, queryConf, fs);
        speculative_repartition(dataset2, dataset2_query, dataset2_queryWindow, dataset2_tableInfo, dataset2_hpinput, dataset2_am, dataset2_scan_blocks, dataset2_iterator_type, queryConf, fs);

        int dataset1_join_attr = dataset1_query.getJoinAttribute();
        int dataset2_join_attr = dataset2_query.getJoinAttribute();

        boolean multiple_iterator_type = false;

        for(int block: dataset1_iterator_type.keySet()){
            if (dataset1_iterator_type.get(block) != dataset1_join_attr){
                multiple_iterator_type = true;
            }
        }

        for(int block: dataset2_iterator_type.keySet()){
            if (dataset2_iterator_type.get(block) != dataset2_join_attr){
                multiple_iterator_type = true;
            }
        }

        // separate read-only and repartitioned. Read only go first.

        if (IntInArray(dataset1_join_attr, dataset1_partitions) && IntInArray(dataset2_join_attr, dataset2_partitions) && dataset2_partitions.length == 1 && multiple_iterator_type == false) {
            // read_index && init over_lap

            read_index(dataset1_bucketInfo, dataset1_am.get(dataset1_join_attr).getIndex(), dataset1_join_attr);
            read_index(dataset2_bucketInfo, dataset2_am.get(dataset2_join_attr).getIndex(), dataset2_join_attr);

            init_overlap();

            if (dataset1_partitions.length == 1) { // hyper join only

                // extract hyper join, update dataset2_iterator_type, shuffle scan on dataset2 in case some data blocks are left.

                extractHyperJoin(dataset1_query, hyperJoinSplit, dataset1_scan_blocks, dataset1_iterator_type, dataset1_hpinput.getPartitionIdSizeMap(), queryConf.getMaxSplitSize());

                hyperjoin = getHyperJoinInput();
                // clear dataset2_scan_blocks

                dataset2_scan_blocks.clear();

                // in case some blocks are left, probably it's empty
                extractShuffleJoin(dataset2_query, shuffleJoinSplit2, dataset2_scan_blocks, dataset2_iterator_type, dataset2_hpinput.getPartitionIdSizeMap(), queryConf.getMaxSplitSize(), queryConf.getWorkerNum());
            } else { // partial hyper join + partial shuffle join

                Map<Integer, Integer> dataset2_iterator_type_copy = new HashMap<Integer, Integer>();
                for (int key : dataset2_iterator_type.keySet()) {
                    dataset2_iterator_type_copy.put(key, dataset2_iterator_type.get(key));
                }

                // copy dataset2_iterator_type,  extract hyper join, update dataset2_iterator_type, any remaining block need repartitioned. all others need to be scanned (using the copy to update scan blocks).

                extractHyperJoin(dataset1_query, hyperJoinSplit, dataset1_scan_blocks, dataset1_iterator_type, dataset1_hpinput.getPartitionIdSizeMap(), queryConf.getMaxSplitSize());

                hyperjoin = getHyperJoinInput();

                // remaining blocks need to be repartitioned, otherwise scan

                for (int key : dataset2_iterator_type_copy.keySet()) {
                    if (dataset2_iterator_type.containsKey(key) == false) {
                        dataset2_scan_blocks.get(dataset2_join_attr).add(dataset2_iterator_type_copy.get(key));
                    }
                }

                // remove joinAttribute from dataset 1

                dataset1_scan_blocks.remove(dataset1_join_attr);

                extractShuffleJoin(dataset1_query, shuffleJoinSplit1, dataset1_scan_blocks, dataset1_iterator_type, dataset1_hpinput.getPartitionIdSizeMap(), queryConf.getMaxSplitSize(), queryConf.getWorkerNum());
                extractShuffleJoin(dataset2_query, shuffleJoinSplit2, dataset2_scan_blocks, dataset2_iterator_type, dataset2_hpinput.getPartitionIdSizeMap(), queryConf.getMaxSplitSize(), queryConf.getWorkerNum());
            }


        } else { // full shuffle join
            extractShuffleJoin(dataset1_query, shuffleJoinSplit1, dataset1_scan_blocks, dataset1_iterator_type, dataset1_hpinput.getPartitionIdSizeMap(), queryConf.getMaxSplitSize(), queryConf.getWorkerNum());
            extractShuffleJoin(dataset2_query, shuffleJoinSplit2, dataset2_scan_blocks, dataset2_iterator_type, dataset2_hpinput.getPartitionIdSizeMap(), queryConf.getMaxSplitSize(), queryConf.getWorkerNum());
        }

        shufflejoin1 = getShuffleJoinInput1();
        shufflejoin2 = getShuffleJoinInput2();
    }

    public static void persistQueryToDisk(JoinQuery q, SparkJoinQueryConf queryConf) {
        String pathToQueries = queryConf.getWorkingDir() + "/" + q.getTable() + "/queries";
        FileSystem fs = HDFSUtils.getFSByHadoopHome(queryConf.getHadoopHome());
        HDFSUtils.safeCreateFile(fs, pathToQueries,
                queryConf.getHDFSReplicationFactor());
        HDFSUtils.appendLine(fs, pathToQueries, q.toString());
    }


    private static void persistIndex(JoinRobustTree index, int partition, TableInfo tableInfo, SparkJoinQueryConf queryConf, FileSystem fs) {
        String pathToIndex = queryConf.getWorkingDir() + "/" + tableInfo.tableName + "/index";

        if (partition != -1) {
            pathToIndex = pathToIndex + "." + partition;
        }

        try {
            if (fs.exists(new Path(pathToIndex))) {
                // If index file exists, move it to a new filename
                long currentMillis = System.currentTimeMillis();
                String oldIndexPath = pathToIndex + "." + currentMillis;
                boolean successRename = fs.rename(new Path(pathToIndex),
                        new Path(oldIndexPath));
                if (!successRename) {
                    System.out.println("Index rename to " + oldIndexPath
                            + " failed");
                }
            }
            HDFSUtils.safeCreateFile(fs, pathToIndex, queryConf.getHDFSReplicationFactor());
        } catch (IOException e) {
            System.out.println("ERR: Writing Index failed: " + e.getMessage());
            e.printStackTrace();
        }

        byte[] indexBytes = index.marshall();
        HDFSUtils.writeFile(fs,
                pathToIndex, queryConf.getHDFSReplicationFactor(), indexBytes, 0,
                indexBytes.length, false);
    }

    private static boolean IntInArray(int value, int[] arr) {
        for (int i = 0; i < arr.length; i++) {
            if (arr[i] == value) {
                return true;
            }
        }
        return false;
    }

    private static ArrayList<Integer> ArrayToArrayList(int[] arr) {
        ArrayList<Integer> result = new ArrayList<Integer>();
        for (int i = 0; i < arr.length; i++) {
            result.add(arr[i]);
        }
        return result;
    }

    private static int[] ArrayListToArray(ArrayList<Integer> arr_list) {
        int[] arr = new int[arr_list.size()];
        for (int i = 0; i < arr.length; i++) {
            arr[i] = arr_list.get(i);
        }
        return arr;
    }

    private static int[] filter_data_blocks(int[] blocks, HPJoinInput hpinput) {
        ArrayList<Integer> result = new ArrayList<Integer>();
        Map<Integer, Long> sizeMap = hpinput.getPartitionIdSizeMap();

        for (int i = 0; i < blocks.length; i++) {
            if (sizeMap.containsKey(blocks[i])) {
                result.add(blocks[i]);
            }
        }
        return ArrayListToArray(result);
    }

    public static void speculative_repartition(String tableName, JoinQuery query, List<JoinQuery> queryWindow, TableInfo tableInfo, HPJoinInput hpinput, Map<Integer, JoinAccessMethod> am_map, Map<Integer, ArrayList<Integer>> scan_blocks, Map<Integer, Integer> iterator_type, SparkJoinQueryConf queryConf, FileSystem fs) {

        int joinAttribute = query.getJoinAttribute();
        int[] partitions = tableInfo.partitions;


        // calculate expected weight for each partition

        Map<Integer, Integer> expected_weights = new HashMap<Integer, Integer>();

        for (int i = 0; i < queryWindow.size(); i++) {
            int attr = queryWindow.get(i).getJoinAttribute();
            if (expected_weights.containsKey(attr) == false) {
                expected_weights.put(attr, 0);
            }
            expected_weights.put(attr, expected_weights.get(attr) + 1);
        }

        for(int i = 0; i < partitions.length; i ++){
            if (expected_weights.containsKey(partitions[i]) == false){
                expected_weights.put(partitions[i], 0);
            }
        }


        int total_weight = 0;

        for (int attr : expected_weights.keySet()) {
            total_weight += expected_weights.get(attr);
        }

        // this will only happen at the beginning

        int noJoinAttr = -1;

        if (queryWindow.size() < Globals.QUERY_WINDOW_SIZE) {
            if (expected_weights.containsKey(noJoinAttr) == false) {
                expected_weights.put(noJoinAttr, 0);
            }
            expected_weights.put(noJoinAttr, Globals.QUERY_WINDOW_SIZE - total_weight);
        }

        // calculate partition size for each index

        Map<Integer, Long> partition_sizes = new HashMap<Integer, Long>();

        // init HPJoinInput && am_map

        queryConf.setJoinQuery(query);

        String input_dir = queryConf.getWorkingDir() + "/" + tableName + "/data";
        hpinput.initialize(listStatus(fs, input_dir));

        long table_size = 0;

        for (int i = 0; i < partitions.length; i++) {
            JoinAccessMethod am = new JoinAccessMethod();
            am.init(queryConf, partitions[i]);
            am_map.put(partitions[i], am);

            int[] bucket_ids = am.getIndex().getAllBuckets();
            Map<Integer, Long> bucket_size = hpinput.getPartitionIdSizeMap();
            long total_size = 0;
            for (int j = 0; j < bucket_ids.length; j++) {
                int bid = bucket_ids[j];
                if (bucket_size.containsKey(bid)) {
                    total_size += bucket_size.get(bid);
                }
            }
            partition_sizes.put(partitions[i], total_size);
            table_size += total_size;
        }


        boolean partitionExists = IntInArray(joinAttribute, partitions);

        if (partitionExists == false) {
            // create a new partitioning tree for the new joinAttribute.

            // step 1: read sample

            ParsedTupleList sample = new ParsedTupleList(tableInfo.getTypeArray());

            String pathToSample = queryConf.getWorkingDir() + "/" + tableName + "/sample";

            try {
                if (fs.exists(new Path(pathToSample))) {
                    byte[] sampleBytes = HDFSUtils.readFile(fs, pathToSample);
                    sample.unmarshall(sampleBytes, tableInfo.delimiter);
                } else {
                    throw new RuntimeException();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }


            // step 2: create a new partitioning tree

            // Construct the index from the sample.
            JoinRobustTree index = new JoinRobustTree(tableInfo);
            index.joinAttributeDepth = (tableInfo.depth + 1) / 2; // ceiling
            index.maxBuckets = 1 << tableInfo.depth;
            index.sample = sample;
            index.numAttributes = tableInfo.getTypeArray().length;
            index.dimensionTypes = tableInfo.getTypeArray();
            index.initProbe(query);


            // step 3: add joinAttribute into tableInfo's partitions and persist

            int[] newPartitions = new int[partitions.length + 1];
            for (int i = 0; i < partitions.length; i++) {
                newPartitions[i] = partitions[i];
            }
            newPartitions[partitions.length] = joinAttribute;
            Arrays.sort(newPartitions);

            tableInfo.partitions = newPartitions;
            tableInfo.save(queryConf.getWorkingDir(), queryConf.getHDFSReplicationFactor(), fs);


            // step 4: persist index

            persistIndex(index, joinAttribute, tableInfo, queryConf, fs);
        }

        // adjust tree -> adapt or delete + scan

        //normalization
        double join_expected_weight = 1.0 * expected_weights.get(joinAttribute) / Globals.QUERY_WINDOW_SIZE ;

        double join_weight = 0;
        if (partition_sizes.containsKey(joinAttribute)) {
            join_weight = 1.0 * partition_sizes.get(joinAttribute) / table_size;
        }

        if (join_weight < join_expected_weight) { // repartition other indexes to joinAttribute

            for (int i = 0; i < partitions.length; i++) {

                //normalization

                double weight = 1.0 * partition_sizes.get(partitions[i]) / table_size;
                double expected_weight = 1.0 * expected_weights.get(partitions[i]) / Globals.QUERY_WINDOW_SIZE ;

                JoinAccessMethod am = am_map.get(partitions[i]);
                JoinRobustTree rt = am.getIndex();

                if (weight > expected_weight) {  // delete

                    long deleteSize = (long) ((weight - expected_weight) * table_size);
                    boolean deleteAll = expected_weights.get(partitions[i]) == 0;

                    // indexScan

                    PartitionSplit scan_split = am.getPartitionSplits(query, true, partitions[i])[0];
                    PartitionSplit delete_split = rt.delete(deleteSize, deleteAll, query, partitions[i], hpinput.getPartitionIdSizeMap());

                    if (deleteAll || rt.getRoot() == null) { // delete index, only one can be deleteAll
                        String pathToIndex = queryConf.getWorkingDir() + "/" + tableInfo.tableName + "/index";

                        if (partitions[i] != -1) {
                            pathToIndex = pathToIndex + "." + partitions[i];
                        }

                        HDFSUtils.deleteFile(fs, pathToIndex, true);

                        int[] newPartitions = new int[partitions.length - 1];
                        for (int k = 0, j = 0; k < partitions.length; k++) {
                            if (partitions[i] != partitions[k]) {
                                newPartitions[j++] = partitions[k];
                            }
                        }

                        Arrays.sort(newPartitions);

                        tableInfo.partitions = newPartitions;
                        tableInfo.save(queryConf.getWorkingDir(), queryConf.getHDFSReplicationFactor(), fs);

                    } else {
                        persistIndex(rt, partitions[i], tableInfo,queryConf,fs);
                    }

                    // filter scan_split by delete_split

                    // some blocks may be empty, need a filter

                    HashSet<Integer> delete_bid = new HashSet<Integer>();
                    int[] bids = filter_data_blocks(delete_split.getPartitions(), hpinput);


                    for (int j = 0; j < bids.length; j++) {
                        delete_bid.add(bids[j]);
                        iterator_type.put(bids[j], joinAttribute);
                    }

                    ArrayList<Integer> filtered_bids = new ArrayList<Integer>();

                    bids = filter_data_blocks(scan_split.getPartitions(), hpinput);
                    for (int j = 0; j < bids.length; j++) {
                        if (delete_bid.contains(bids[j]) == false) {
                            filtered_bids.add(bids[j]);
                        }
                    }
                    scan_blocks.put(partitions[i], filtered_bids);


                } else { // scan
                    PartitionSplit[] splits = am.getPartitionSplits(query, false, partitions[i]);
                    scan_blocks.put(partitions[i], ArrayToArrayList(filter_data_blocks(splits[0].getPartitions(), hpinput)));

                    if (splits.length > 1) {
                        int[] repartition_bids = filter_data_blocks(splits[1].getPartitions(), hpinput);

                        for (int j = 0; j < repartition_bids.length; j++) {
                            iterator_type.put(repartition_bids[j], partitions[i]);
                        }
                    }
                }
            }
        } else {

            for (int i = 0; i < partitions.length; i++) {
                JoinAccessMethod am = am_map.get(partitions[i]);
                PartitionSplit[] splits = am.getPartitionSplits(query, false, partitions[i]);

                scan_blocks.put(partitions[i], ArrayToArrayList(filter_data_blocks(splits[0].getPartitions(), hpinput)));

                if (splits.length > 1) {
                    int[] repartition_bids = filter_data_blocks(splits[1].getPartitions(), hpinput);
                    for (int j = 0; j < repartition_bids.length; j++) {
                        iterator_type.put(repartition_bids[j], partitions[i]);
                    }
                }

            }
        }
    }

    public void setupTableInfo() {
        Globals.loadTableInfo(dataset1, queryConf.getWorkingDir(), fs);
        Globals.loadTableInfo(dataset2, queryConf.getWorkingDir(), fs);

        dataset1_tableInfo = Globals.getTableInfo(dataset1);
        dataset2_tableInfo = Globals.getTableInfo(dataset2);

    }

    public static List<JoinQuery> loadQueries(String tableName, SparkJoinQueryConf queryConf) {
        FileSystem fs = HDFSUtils.getFSByHadoopHome(queryConf.getHadoopHome());
        String pathToQueries = queryConf.getWorkingDir() + "/" + tableName + "/queries";

        List<JoinQuery> queryWindow = new ArrayList<JoinQuery>();

        try {
            if (fs.exists(new Path(pathToQueries))) {
                byte[] queryBytes = HDFSUtils.readFile(fs, pathToQueries);
                String queries = new String(queryBytes);
                Scanner sc = new Scanner(queries);
                while (sc.hasNextLine()) {
                    String query = sc.nextLine();
                    JoinQuery f = new JoinQuery(query);
                    queryWindow.add(f);
                }

                if (queryWindow.size() > Globals.QUERY_WINDOW_SIZE - 1) {
                    // set windows size
                    queryWindow = queryWindow.subList(queryWindow.size() - (Globals.QUERY_WINDOW_SIZE - 1), queryWindow.size());
                }
                sc.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return queryWindow;
    }

    public void extractHyperJoin(JoinQuery dataset_query, ArrayList<PartitionSplit> hyperJoinSplit, Map<Integer, ArrayList<Integer>> scan_blocks, Map<Integer, Integer> iterator_type, Map<Integer, Long> partitionSizes, long maxSplitSize) {
        ArrayList<Integer> alist = scan_blocks.get(dataset_query.getJoinAttribute());
        PartitionIterator pi = new PostFilterIterator(dataset_query.castToQuery());


        int total = 0;
        ArrayList<PartitionSplit> hyper_splits = groupSplits(pi, ArrayListToArray(alist), partitionSizes, maxSplitSize);
        for (PartitionSplit hs : hyper_splits) {
            hyperJoinSplit.add(hs);
            total += hs.getPartitions().length;
        }

        if (total != alist.size()) {
            throw new RuntimeException("some partition is lost");
        }

        Map<Integer, ArrayList<Integer>> blocks = new HashMap<Integer, ArrayList<Integer>>();

        for (int block : iterator_type.keySet()) {
            int index = iterator_type.get(block);
            if (blocks.containsKey(index) == false) {
                blocks.put(index, new ArrayList<Integer>());
            }
            blocks.get(index).add(block);
        }

        for (int index : blocks.keySet()) {
            alist = blocks.get(index);
            pi = new JoinRepartitionIterator(dataset_query.castToQuery(), index);
            total = 0;
            hyper_splits = groupSplits(pi, ArrayListToArray(alist), partitionSizes, maxSplitSize);
            for (PartitionSplit hs : hyper_splits) {
                hyperJoinSplit.add(hs);
                total += hs.getPartitions().length;
            }

            if (total != alist.size()) {
                throw new RuntimeException("some partition is lost");
            }
        }
    }

    public static void extractShuffleJoin(JoinQuery dataset_query, ArrayList<PartitionSplit> shuffleJoinSplit, Map<Integer, ArrayList<Integer>> scan_blocks, Map<Integer, Integer> iterator_type, Map<Integer, Long> partitionSizes, long maxSplitSize, int worker_num) {

        for (int index : scan_blocks.keySet()) {
            // construct a PostFilterIterator
            PartitionIterator pi = new PostFilterIterator(dataset_query.castToQuery());
            ArrayList<PartitionSplit> shuffle_splits = resizeSplits(pi, ArrayListToArray(scan_blocks.get(index)), partitionSizes, maxSplitSize, dataset_query, -1);
            int total = 0;
            for (PartitionSplit hs : shuffle_splits) {
                shuffleJoinSplit.add(hs);
                total += hs.getPartitions().length;
            }
            if (total != scan_blocks.get(index).size()) {
                throw new RuntimeException("some partition is lost");
            }

        }

        // for each index, construct a JoinRepartitionIterator

        Map<Integer, ArrayList<Integer>> repartition_blocks = new HashMap<Integer, ArrayList<Integer>>();

        for (int block : iterator_type.keySet()) {
            int index = iterator_type.get(block);
            if (repartition_blocks.containsKey(index) == false) {
                repartition_blocks.put(index, new ArrayList<Integer>());
            }
            repartition_blocks.get(index).add(block);
        }

        for (int index : repartition_blocks.keySet()) {
            ArrayList<Integer> alist = repartition_blocks.get(index);
            PartitionIterator pi = new JoinRepartitionIterator(dataset_query.castToQuery(), index);
            ArrayList<PartitionSplit> shuffle_splits = resizeSplits(pi, ArrayListToArray(alist), partitionSizes, maxSplitSize, dataset_query, worker_num);

            int total = 0;
            for (PartitionSplit hs : shuffle_splits) {
                shuffleJoinSplit.add(hs);
                total += hs.getPartitions().length;
            }
            if (total != repartition_blocks.get(index).size()) {
                throw new RuntimeException("some partition is lost");
            }
        }
    }

    /*
    private void extractJoin(JoinQuery dataset1_query, PartitionSplit[] dataset1_splits, Map<Integer, Long> dataset1_partitionSizes, JoinQuery dataset2_query, PartitionSplit[] dataset2_splits, Map<Integer, Long> dataset2_partitionSizes, long maxSplitSize) {
        // TODO: the threshold should be on the number of blocks on LHS, not RHS, since we are building hashtable on LHS

        HashMap<Integer, Integer> rightCounters = new HashMap<Integer, Integer>();


        for (int i = 0; i < dataset1_splits.length; i++) {
            PartitionSplit split = dataset1_splits[i];
            int[] bids = split.getPartitions();

            for (int j = 0; j < bids.length; j++) {
                ArrayList<Integer> dep_bids = overlap_chunks.get(bids[j]);

                for (int dep_id : dep_bids) {
                    if (rightCounters.containsKey(dep_id) == false) {
                        rightCounters.put(dep_id, 0);
                    }
                    rightCounters.put(dep_id, rightCounters.get(dep_id) + 1);
                }

            }
        }

        // statistics

        int[] hist = new int[16];
        for (int chunk : rightCounters.keySet()) {
            int i = 0;
            while ((1 << i) < rightCounters.get(chunk)) {
                i++;
            }
            hist[i]++;
        }

        System.out.println("Histogram:");
        for (int i = 0; i < 16; i++) {
            System.out.println((1 << i) + ": " + hist[i]);
        }


        if (hyperjoin) {
            for (int i = 0; i < dataset1_splits.length; i++) {
                PartitionSplit split = dataset1_splits[i];
                int[] bids = split.getPartitions();

                ArrayList<Integer> hyper_ids = new ArrayList<Integer>();

                for (int j = 0; j < bids.length; j++) {
                    hyper_ids.add(bids[j]);
                }

                if (hyper_ids.size() > 0) {
                    int[] hyper_ids_int = new int[hyper_ids.size()];
                    for (int j = 0; j < hyper_ids_int.length; j++) {
                        hyper_ids_int[j] = hyper_ids.get(j);
                    }

                    int total = 0;
                    ArrayList<PartitionSplit> hyper_splits = groupSplits(split.getIterator(), hyper_ids_int, dataset1_partitionSizes, maxSplitSize);
                    for (PartitionSplit hs : hyper_splits) {
                        hyperJoinSplit.add(hs);
                        total += hs.getPartitions().length;
                    }

                    if (total != hyper_ids_int.length) {
                        throw new RuntimeException("some partition is lost");
                    }
                }
            }
        } else {

            extractShuffleJoin(dataset1_query, dataset1_splits, dataset1_partitionSizes, shuffleJoinSplit1, maxSplitSize, queryConf.getWorkerNum());
            extractShuffleJoin(dataset2_query, dataset2_splits, dataset2_partitionSizes, shuffleJoinSplit2, maxSplitSize, queryConf.getWorkerNum());
        }
    }

*/

    private PartitionSplit[] getPartitionSplits(int[] bids, JoinQuery q) {
        PostFilterIterator pi = new PostFilterIterator(q.castToQuery());
        PartitionSplit psplit = new PartitionSplit(bids, pi);
        PartitionSplit[] ps = new PartitionSplit[1];
        ps[0] = psplit;
        return ps;
    }


    public String getHyperJoinInput() {
        // iter1_type, id1:len1:id2:len2:... , id1:len1:iter2_type:id2:len2:iter2_type... ;...
        StringBuilder sb = new StringBuilder();

        for (PartitionSplit split : hyperJoinSplit) {

            if(split.getPartitions().length == 0) continue;

            if (sb.length() > 0) {
                sb.append(";");
            }

            PartitionIterator iter = split.getIterator();
            if (iter instanceof PostFilterIterator) {
                sb.append(-2 + ",");
            } else {
                JoinRepartitionIterator pi = (JoinRepartitionIterator) iter;
                sb.append(pi.getIndexPartition() + ",");
            }

            int[] bucketIds = split.getPartitions();
            int[] dep_bucketIDs = getOverlappedSplits(bucketIds);

            long[] bucket_lens = dataset1_hpinput.getLengths(bucketIds);
            long[] dep_bucket_lens = dataset2_hpinput.getLengths(dep_bucketIDs);

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
                int type = -2;

                if (dataset2_iterator_type.containsKey(dep_bucketIDs[i])) {
                    type = dataset2_iterator_type.get(dep_bucketIDs[i]);
                    dataset2_iterator_type.remove(dep_bucketIDs[i]);// we can only use JoinRepartitionIterator once.
                }

                sb.append(dep_bucketIDs[i] + ":" + dep_bucket_lens[i] + ":" + type);
            }
        }

        return sb.toString();
    }

    public static String getShuffleJoinInputHelper(ArrayList<PartitionSplit> shuffleJoinSplit, HPJoinInput hpinput) {
        // iter,1:100,2:120; ...

        StringBuilder sb = new StringBuilder();

        for (PartitionSplit split : shuffleJoinSplit) {

            if(split.getPartitions().length == 0) continue;

            if (sb.length() > 0) {
                sb.append(";");
            }

            PartitionIterator iter = split.getIterator();
            if (iter instanceof PostFilterIterator) {
                sb.append(-2 + ",");
            } else {
                JoinRepartitionIterator pi = (JoinRepartitionIterator) iter;
                sb.append(pi.getIndexPartition() + ",");
            }

            int[] bucketIds = split.getPartitions();

            long[] bucket_lens = hpinput.getLengths(bucketIds);

            for (int i = 0; i < bucketIds.length; i++) {
                if (i > 0) {
                    sb.append(",");
                }
                sb.append(bucketIds[i] + ":" + bucket_lens[i]);
            }
        }

        return sb.toString();
    }

    public String getShuffleJoinInput1() {
        return getShuffleJoinInputHelper(shuffleJoinSplit1, dataset1_hpinput);
    }

    public String getShuffleJoinInput2() {
        //1:100:type;2:120:type
        return getShuffleJoinInputHelper(shuffleJoinSplit2, dataset2_hpinput);
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


    public static List<FileStatus> listStatus(FileSystem fs, String path) {
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

    public static int[] expandPartitionSplit(PartitionSplit[] splits) {
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
                info.put(bucket_id, new MDIndex.BucketInfo(TypeUtils.TYPE.LONG, null, null));
            } else {
                info.put(bucket_id, bucketRanges.get(bucket_id));
            }
        }
    }


    private void init_overlap() {
        // filtering

        HashSet<Integer> splits1 = new HashSet<Integer>();


        for (int block : dataset1_scan_blocks.get(dataset1_query.getJoinAttribute())) {
            splits1.add(block);
        }

        for(int block: dataset1_iterator_type.keySet()){
            splits1.add(block);
        }

        HashSet<Integer> splits2 = new HashSet<Integer>();

        for (int block : dataset2_scan_blocks.get(dataset2_query.getJoinAttribute())) {
            splits2.add(block);
        }

        for(int block: dataset2_iterator_type.keySet()){
            splits2.add(block);
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


    private void init_bucketInfo(JoinAccessMethod jam1, JoinAccessMethod jam2) {
        dataset1_bucketInfo = new HashMap<Integer, MDIndex.BucketInfo>();
        dataset2_bucketInfo = new HashMap<Integer, MDIndex.BucketInfo>();

        overlap_chunks = new HashMap<Integer, ArrayList<Integer>>();


        //System.out.println("Populate dataset1_bucketInfo");


        read_index(dataset1_bucketInfo, jam1.opt.getIndex(), dataset1_query.getJoinAttribute());


        //Globals.schema =  Schema.createSchema(dataset2_schema);

        //System.out.println("Populate dataset2_bucketInfo");


        read_index(dataset2_bucketInfo, jam2.opt.getIndex(), dataset2_query.getJoinAttribute());


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
        /*
        if (vals == null){
            return 0;
        }
        */
        int sum = 0;
        for (int i = 0; i < vals.size(); i++) {
            if (chunks.contains(vals.get(i))) {
                sum++;
            }
        }
        return sum;
    }


    public static ArrayList<PartitionSplit> resizeSplits(PartitionIterator partitionIter, int[] bids, Map<Integer, Long> partitionSizes, long maxSplitSize, JoinQuery dataset_query, int worker_num) {

        // the size of a split could be larger than maxSplitSize, just in case a singel file is largen than this.

        ArrayList<PartitionSplit> resizedSplits = new ArrayList<PartitionSplit>();

        if (worker_num != -1) {
            // split input into WORKER_NUM splits

            ArrayList<ArrayList<Integer>> array_bids = new ArrayList<ArrayList<Integer>>();

            for (int i = 0; i < worker_num; i++) {
                array_bids.add(new ArrayList<Integer>());
            }

            for (int i = 0; i < bids.length; i++) {
                array_bids.get(i % worker_num).add(bids[i]);
            }

            for (int i = 0; i < worker_num; i++) {

                ArrayList<Integer> array_bid = array_bids.get(i);

                int[] split_bids = new int[array_bid.size()];
                for (int j = 0; j < array_bid.size(); j++) {
                    split_bids[j] = array_bid.get(j);
                }
                PartitionSplit split = new PartitionSplit(split_bids, partitionIter);
                resizedSplits.add(split);
            }
            return resizedSplits;
        } else {
            int it = 0;
            long totalsize = 0;
            ArrayList<Integer> cur_split = new ArrayList<Integer>();

            while (it < bids.length) {
                if (partitionSizes.get(bids[it]) == null) {
                    System.out.println(bids[it] + " is empty!");
                }
                long cur_size = partitionSizes.get(bids[it]);

                totalsize += cur_size;
                cur_split.add(bids[it]);

                if (totalsize >= maxSplitSize) {
                    int[] split_bids = new int[cur_split.size()];
                    for (int i = 0; i < split_bids.length; i++) {
                        split_bids[i] = cur_split.get(i);
                    }
                    PartitionSplit split = new PartitionSplit(split_bids, partitionIter);
                    resizedSplits.add(split);
                    cur_split = new ArrayList<Integer>();
                    totalsize = 0;
                }
                it++;
            }

            if (cur_split.size() > 0) {
                int[] split_bids = new int[cur_split.size()];
                for (int i = 0; i < split_bids.length; i++) {
                    split_bids[i] = cur_split.get(i);
                }
                PartitionSplit split = new PartitionSplit(split_bids, partitionIter);
                resizedSplits.add(split);
            }

            int sum = 0;

            for (PartitionSplit split : resizedSplits) {
                sum += split.getPartitions().length;
            }

            if (sum != bids.length) {
                throw new RuntimeException("miss some data blocks in resizeSplits");
            }

            return resizedSplits;
        }

    }


    /* the following code uses heuristic grouping
       TODO: add different grouping algos
    */

    private ArrayList<PartitionSplit> groupSplits(PartitionIterator partitionIter, int[] bids, Map<Integer, Long> partitionSizes, long maxSplitSize) {
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
            long splitAvailableSize = maxSplitSize;
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

                if (partitionSizes.get(bucket) == null) {
                    System.out.println(bucket + " is empty!");
                }

                splitAvailableSize -= partitionSizes.get(bucket);


                cur_split.add(bucket);
                for (int rhs : overlap_chunks.get(bucket)) {
                    chunks.add(rhs);
                }
                buckets.remove(best_offset);
                size--;

            }

            int[] split_bids = new int[cur_split.size()];
            for (int i = 0; i < split_bids.length; i++) {
                split_bids[i] = cur_split.get(i);
            }

            PartitionSplit split = new PartitionSplit(split_bids, partitionIter);
            resizedSplits.add(split);
            //System.out.println("split size: " + totalSize + " " + Arrays.toString(split_bids));
        }

        int sum = 0;

        for (PartitionSplit split : resizedSplits) {
            sum += split.getPartitions().length;
        }

        if (sum != bids.length) {
            throw new RuntimeException("miss some data blocks in groupSplits");
        }


        return resizedSplits;
    }


    private long getPartitionSplitSize(PartitionSplit split,
                                       Map<Integer, Long> partitionIdSizeMap) {

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
