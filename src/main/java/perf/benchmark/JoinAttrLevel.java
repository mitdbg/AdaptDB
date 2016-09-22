package perf.benchmark;

import core.adapt.JoinQuery;
import core.adapt.Predicate;
import core.common.globals.Globals;
import core.common.globals.Schema;
import core.common.globals.TableInfo;
import core.common.index.JRNode;
import core.common.index.JoinRobustTree;
import core.common.index.MDIndex;
import core.common.key.ParsedTupleList;
import core.utils.ConfUtils;
import core.utils.HDFSUtils;
import core.utils.TypeUtils;
import org.apache.hadoop.fs.FileSystem;

import java.util.*;

import java.util.BitSet;

/**
 * Created by ylu on 1/21/16.
 */

public class JoinAttrLevel {

    public static String lineitem = "lineitem";
    public static String orders = "orders";
    public static String customer = "customer";
    public static String part = "part";

    public static int lineitemBuckets = 16384, ordersBuckets = 2048, customerBuckets = 512, partBuckets = 512;

    public static ParsedTupleList lineitemSample, ordersSample, customerSample, partSample;

    public static ParsedTupleList loadSample(String tableName) {

        ConfUtils cfg = new ConfUtils("/Users/ylu/Documents/workspace/AdaptDB/conf/local.properties");
        FileSystem fs = HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME());
        // Load table info.
        Globals.loadTableInfo(tableName, cfg.getHDFS_WORKING_DIR(), fs);
        TableInfo tableInfo = Globals.getTableInfo(tableName);

        String pathToSample = cfg.getHDFS_WORKING_DIR() + "/" + tableInfo.tableName + "/sample";

        byte[] sampleBytes = HDFSUtils.readFile(fs, pathToSample);

        // read all the sample files and put them into the sample key set
        ParsedTupleList sample = new ParsedTupleList(tableInfo.getTypeArray());
        sample.unmarshall(sampleBytes, tableInfo.delimiter);

        return sample;
    }

    public static void init() {
        lineitemSample = loadSample(lineitem);
        ordersSample = loadSample(orders);
        customerSample = loadSample(customer);
        partSample = loadSample(part);
    }

    public static JoinRobustTree getAdaptDbIndex(String tableName, ParsedTupleList sample, int numBuckets, int levelsForJoinAttr, int joinAttr) {

        MDIndex.Bucket.maxBucketId = 0;

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

    public static JoinRobustTree getAdaptDbIndexWithQ(String tableName, ParsedTupleList sample, JoinQuery q, int numBuckets, int levelsForJoinAttr, int joinAttr) {

        MDIndex.Bucket.maxBucketId = 0;

        TableInfo tableInfo = Globals.getTableInfo(tableName);

        JoinRobustTree rt = new JoinRobustTree(tableInfo);
        rt.joinAttributeDepth = levelsForJoinAttr;
        rt.setMaxBuckets(numBuckets);
        rt.loadSample(sample);
        rt.initProbe(q);

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

    public static void read_index(Map<Integer, MDIndex.BucketInfo> info, JoinRobustTree rt, JoinQuery q) {
        // If a particular bucket is not in the following map, then the range is (-oo,+oo).

        int attr = q.getJoinAttribute();

        Map<Integer, MDIndex.BucketInfo> bucketRanges = rt.getBucketRanges(attr);

        List<JRNode> nodes = rt.getMatchingBuckets(q.getPredicates());

        int[] buckets = new int[nodes.size()];

        int idx = 0;
        for (JRNode node : nodes) {
            buckets[idx++] = node.bucket.getBucketId();
        }

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

    public static int getIntersectionSize(BitSet a, BitSet b) {
        BitSet ca = (BitSet) a.clone();
        BitSet cb = (BitSet) b.clone();
        ca.and(cb);
        return ca.cardinality();
    }


    public static int getCost(JoinRobustTree rt1, JoinRobustTree rt2, int joinAttr1, int joinAttr2, int bufferSize) {
        Map<Integer, MDIndex.BucketInfo> dataset1_bucketInfo = new HashMap<Integer, MDIndex.BucketInfo>();
        Map<Integer, MDIndex.BucketInfo> dataset2_bucketInfo = new HashMap<Integer, MDIndex.BucketInfo>();

        read_index(dataset1_bucketInfo, rt1, joinAttr1);
        read_index(dataset2_bucketInfo, rt2, joinAttr2);

        BitSet[] overlap_chunks = new BitSet[dataset1_bucketInfo.keySet().size()];

        for (int i = 0; i < overlap_chunks.length; i++) {
            overlap_chunks[i] = new BitSet();
        }

        for (Integer i : dataset1_bucketInfo.keySet()) {
            MDIndex.BucketInfo info_i = dataset1_bucketInfo.get(i);
            for (Integer j : dataset2_bucketInfo.keySet()) {
                MDIndex.BucketInfo info_j = dataset2_bucketInfo.get(j);
                if (info_i.overlap(info_j)) {
                    overlap_chunks[i].set(j);
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
            BitSet chunks = new BitSet();
            int splitAvailableSize = bufferSize;
            while (size > 0 && splitAvailableSize > 0) {
                int maxIntersection = -1;
                int best_offset = -1;

                ListIterator<Integer> it = buckets.listIterator();
                int offset = 0;

                while (it.hasNext()) {
                    int value = it.next();
                    if (maxIntersection == -1) {
                        maxIntersection = getIntersectionSize(chunks, overlap_chunks[value]);
                        best_offset = offset;
                    } else {
                        int curIntersection = getIntersectionSize(chunks, overlap_chunks[value]);
                        if (curIntersection > maxIntersection) {
                            maxIntersection = curIntersection;
                            best_offset = offset;
                        }
                    }
                    offset++;
                }
                int bucket = buckets.get(best_offset);

                splitAvailableSize--;


                cur_split.add(bucket);

                chunks.or(overlap_chunks[bucket]);
                buckets.remove(best_offset);
                size--;

            }

            cost += chunks.cardinality();
        }

        return cost;
    }


    public static void LineitemJoinOrders() {
        int bufferSize = 128; // 4G / 32 MB

        for (int i = 0; i <= 14; i++) {
            JoinRobustTree lineitemTree = getAdaptDbIndex(lineitem, lineitemSample, lineitemBuckets, i, 0);

            for (int j = 0; j <= 11; j++) {
                JoinRobustTree ordersTree = getAdaptDbIndex(orders, ordersSample, ordersBuckets, j, 0);
                int cost = getCost(lineitemTree, ordersTree, 0, 0, bufferSize);
                System.out.printf("lineitem %d orders %d cost %d\n", i, j, cost);
            }
        }
    }

    public static void OrdersJoinCustomer() {
        int bufferSize = 128; // 4G / 32 MB
        for (int i = 0; i <= 11; i++) {
            JoinRobustTree ordersTree = getAdaptDbIndex(orders, ordersSample, ordersBuckets, i, 1);

            for (int j = 0; j <= 9; j++) {
                JoinRobustTree customerTree = getAdaptDbIndex(customer, customerSample, customerBuckets, j, 0);
                int cost = getCost(ordersTree, customerTree, 1, 0, bufferSize);
                System.out.printf("orders %d customer %d cost %d\n", i, j, cost);
            }
        }

    }

    public static void LineitemJoinPart() {
        int bufferSize = 128; // 4G / 32 MB

        for (int i = 0; i <= 14; i++) {
            JoinRobustTree lineitemTree = getAdaptDbIndex(lineitem, lineitemSample, lineitemBuckets, i, 1);

            for (int j = 0; j <= 9; j++) {
                JoinRobustTree partTree = getAdaptDbIndex(part, partSample, partBuckets, j, 0);
                int cost = getCost(lineitemTree, partTree, 1, 0, bufferSize);
                System.out.printf("lineitem %d part %d cost %d\n", i, j, cost);
            }
        }
    }


    public static int getCost(JoinRobustTree rt1, JoinRobustTree rt2, JoinQuery q1, JoinQuery q2, int joinAttr1, int joinAttr2, int bufferSize) {

        // q

        Map<Integer, MDIndex.BucketInfo> dataset1_bucketInfo = new HashMap<Integer, MDIndex.BucketInfo>();
        Map<Integer, MDIndex.BucketInfo> dataset2_bucketInfo = new HashMap<Integer, MDIndex.BucketInfo>();

        read_index(dataset1_bucketInfo, rt1, q1);
        read_index(dataset2_bucketInfo, rt2, q2);

        int maxValue = 0;
        for (int i : dataset1_bucketInfo.keySet()) {
            maxValue = Math.max(i, maxValue);
        }


        BitSet[] overlap_chunks = new BitSet[maxValue + 1];


        for (int i = 0; i < overlap_chunks.length; i++) {
            overlap_chunks[i] = new BitSet();
        }

        for (Integer i : dataset1_bucketInfo.keySet()) {
            MDIndex.BucketInfo info_i = dataset1_bucketInfo.get(i);
            for (Integer j : dataset2_bucketInfo.keySet()) {
                MDIndex.BucketInfo info_j = dataset2_bucketInfo.get(j);
                if (info_i.overlap(info_j)) {
                    overlap_chunks[i].set(j);
                }
            }
        }

        int cost = 0;

        List<JRNode> nodes = rt1.getMatchingBuckets(q1.getPredicates());

        int[] bids = new int[nodes.size()];

        int idx = 0;
        for (JRNode node : nodes) {
            bids[idx++] = node.bucket.getBucketId();
        }
        int size = bids.length;

        LinkedList<Integer> buckets = new LinkedList<Integer>();

        for (int i = 0; i < bids.length; i++) {
            buckets.add(bids[i]);
        }

        while (size > 0) {
            ArrayList<Integer> cur_split = new ArrayList<Integer>();
            BitSet chunks = new BitSet();
            int splitAvailableSize = bufferSize;
            while (size > 0 && splitAvailableSize > 0) {
                int maxIntersection = -1;
                int best_offset = -1;

                ListIterator<Integer> it = buckets.listIterator();
                int offset = 0;

                while (it.hasNext()) {
                    int value = it.next();
                    if (maxIntersection == -1) {
                        maxIntersection = getIntersectionSize(chunks, overlap_chunks[value]);
                        best_offset = offset;
                    } else {
                        int curIntersection = getIntersectionSize(chunks, overlap_chunks[value]);
                        if (curIntersection > maxIntersection) {
                            maxIntersection = curIntersection;
                            best_offset = offset;
                        }
                    }
                    offset++;
                }
                int bucket = buckets.get(best_offset);

                splitAvailableSize--;


                cur_split.add(bucket);

                chunks.or(overlap_chunks[bucket]);
                buckets.remove(best_offset);
                size--;

            }

            cost += chunks.cardinality();
        }

        return cost;
    }


    public static void LineitemJoinOrdersWithQ3() {

        // tpch3

        Random rand = new Random();
        String[] mktSegmentVals = new String[]{"AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY"};

        String stringLineitem = "l_orderkey long, l_partkey int, l_suppkey int, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate date, l_commitdate date, l_receiptdate date, l_shipinstruct string, l_shipmode string";
        String stringOrders = "o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate date, o_orderpriority string, o_clerk string, o_shippriority int";

        Schema schemaLineitem = Schema.createSchema(stringLineitem);
        Schema schemaOrders = Schema.createSchema(stringOrders);

        int rand_3 = rand.nextInt(mktSegmentVals.length);
        String c_mktsegment = mktSegmentVals[rand_3];
        Calendar c = new GregorianCalendar();
        int dateOffset = (int) (rand.nextFloat() * (31 + 28 + 31));
        c.set(1995, Calendar.MARCH, 01);
        c.add(Calendar.DAY_OF_MONTH, dateOffset);
        TypeUtils.SimpleDate d3 = new TypeUtils.SimpleDate(c.get(Calendar.YEAR),
                c.get(Calendar.MONTH), c.get(Calendar.DAY_OF_MONTH));


        Predicate p2_3 = new Predicate(schemaOrders.getAttributeId("o_orderdate"), TypeUtils.TYPE.DATE, d3, Predicate.PREDTYPE.LT);
        Predicate p3_3 = new Predicate(schemaLineitem.getAttributeId("l_shipdate"), TypeUtils.TYPE.DATE, d3, Predicate.PREDTYPE.GT);


        JoinQuery q_o = new JoinQuery(orders, schemaOrders.getAttributeId("o_orderkey"), new Predicate[]{p2_3});
        JoinQuery q_l = new JoinQuery(lineitem, schemaLineitem.getAttributeId("l_orderkey"), new Predicate[]{p3_3});


        int bufferSize = 128; // 4G / 32 MB


        for (int i = 0; i <= 14; i++) {
            JoinRobustTree lineitemTree = getAdaptDbIndexWithQ(lineitem, lineitemSample, q_l, lineitemBuckets, i, 0);

            for (int j = 0; j <= 11; j++) {
                JoinRobustTree ordersTree = getAdaptDbIndexWithQ(orders, ordersSample, q_o, ordersBuckets, j, 0);
                int cost = getCost(lineitemTree, ordersTree, q_l, q_o, 0, 0, bufferSize);
                System.out.printf("tpch3: lineitem %d orders %d cost %d\n", i, j, cost);
            }
        }
    }


    public static void LineitemJoinOrdersWithQ10() {

        // tpch10

        Random rand = new Random();


        String stringLineitem = "l_orderkey long, l_partkey int, l_suppkey int, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate date, l_commitdate date, l_receiptdate date, l_shipinstruct string, l_shipmode string";
        String stringOrders = "o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate date, o_orderpriority string, o_clerk string, o_shippriority int";

        Schema schemaLineitem = Schema.createSchema(stringLineitem);
        Schema schemaOrders = Schema.createSchema(stringOrders);


        String l_returnflag_10 = "R";
        String l_returnflag_prev_10 = "N";
        int year_10 = 1993;
        int monthOffset = rand.nextInt(24);
        TypeUtils.SimpleDate d10_1 = new TypeUtils.SimpleDate(year_10 + monthOffset / 12, monthOffset % 12 + 1, 1);
        monthOffset = monthOffset + 3;
        TypeUtils.SimpleDate d10_2 = new TypeUtils.SimpleDate(year_10 + monthOffset / 12, monthOffset % 12 + 1, 1);
        Predicate p1_10 = new Predicate(schemaLineitem.getAttributeId("l_returnflag"), TypeUtils.TYPE.STRING, l_returnflag_10, Predicate.PREDTYPE.LEQ);
        Predicate p4_10 = new Predicate(schemaLineitem.getAttributeId("l_returnflag"),TypeUtils. TYPE.STRING, l_returnflag_prev_10,  Predicate.PREDTYPE.GT);
        Predicate p2_10 = new Predicate(schemaOrders.getAttributeId("o_orderdate"), TypeUtils.TYPE.DATE, d10_1,  Predicate.PREDTYPE.GEQ);
        Predicate p3_10 = new Predicate(schemaOrders.getAttributeId("o_orderdate"), TypeUtils.TYPE.DATE, d10_2,  Predicate.PREDTYPE.LT);

        JoinQuery q_l = new JoinQuery(lineitem, schemaLineitem.getAttributeId("l_orderkey"), new Predicate[]{p1_10, p4_10});
        JoinQuery q_o = new JoinQuery(orders, schemaOrders.getAttributeId("o_orderkey"), new Predicate[]{p2_10, p3_10});


        int bufferSize = 128; // 4G / 32 MB


        for (int i = 0; i <= 14; i++) {
            JoinRobustTree lineitemTree = getAdaptDbIndexWithQ(lineitem, lineitemSample, q_l, lineitemBuckets, i, 0);

            for (int j = 0; j <= 11; j++) {
                JoinRobustTree ordersTree = getAdaptDbIndexWithQ(orders, ordersSample, q_o, ordersBuckets, j, 0);
                int cost = getCost(lineitemTree, ordersTree, q_l, q_o, 0, 0, bufferSize);
                System.out.printf("tpch10: lineitem %d orders %d cost %d\n", i, j, cost);
            }
        }
    }


    public static void LineitemJoinPartWithQ() {

        // tpch19

        Random rand = new Random();
        String brand_19 = "Brand#" + (rand.nextInt(5) + 1) + "" + (rand.nextInt(5) + 1);
        String shipInstruct_19 = "DELIVER IN PERSON";
        double quantity_19 = rand.nextInt(10) + 1;

        String stringLineitem = "l_orderkey long, l_partkey int, l_suppkey int, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate date, l_commitdate date, l_receiptdate date, l_shipinstruct string, l_shipmode string";
        String stringPart = "p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double";
        Schema schemaLineitem = Schema.createSchema(stringLineitem);
        Schema schemaPart = Schema.createSchema(stringPart);


        Predicate p1_19 = new Predicate(schemaLineitem.getAttributeId("l_shipinstruct"), TypeUtils.TYPE.STRING, shipInstruct_19, Predicate.PREDTYPE.EQ);
        Predicate p2_19 = new Predicate(schemaPart.getAttributeId("p_brand"), TypeUtils.TYPE.STRING, brand_19, Predicate.PREDTYPE.EQ);
        Predicate p3_19 = new Predicate(schemaPart.getAttributeId("p_container"), TypeUtils.TYPE.STRING, "SM CASE", Predicate.PREDTYPE.EQ);
        Predicate p4_19 = new Predicate(schemaLineitem.getAttributeId("l_quantity"), TypeUtils.TYPE.DOUBLE, quantity_19, Predicate.PREDTYPE.GT);
        quantity_19 += 10;
        Predicate p5_19 = new Predicate(schemaLineitem.getAttributeId("l_quantity"), TypeUtils.TYPE.DOUBLE, quantity_19, Predicate.PREDTYPE.LEQ);
        Predicate p6_19 = new Predicate(schemaPart.getAttributeId("p_size"), TypeUtils.TYPE.INT, 1, Predicate.PREDTYPE.GEQ);
        Predicate p7_19 = new Predicate(schemaPart.getAttributeId("p_size"), TypeUtils.TYPE.INT, 5, Predicate.PREDTYPE.LEQ);
        Predicate p8_19 = new Predicate(schemaLineitem.getAttributeId("l_shipmode"), TypeUtils.TYPE.STRING, "AIR", Predicate.PREDTYPE.LEQ);

        JoinQuery q_l = new JoinQuery(lineitem, schemaLineitem.getAttributeId("l_partkey"), new Predicate[]{p1_19, p4_19, p5_19, p8_19});
        JoinQuery q_p = new JoinQuery(part, schemaPart.getAttributeId("p_partkey"), new Predicate[]{p2_19, p3_19, p6_19, p7_19});

        int bufferSize = 128; // 4G / 32 MB


        for (int i = 0; i <= 14; i++) {
            JoinRobustTree lineitemTree = getAdaptDbIndexWithQ(lineitem, lineitemSample, q_l, lineitemBuckets, i, 1);

            for (int j = 0; j <= 11; j++) {
                JoinRobustTree partTree = getAdaptDbIndexWithQ(orders, partSample, q_p, partBuckets, j, 0);
                int cost = getCost(lineitemTree, partTree, q_l, q_p, 1, 0, bufferSize);
                System.out.printf("tpch19: lineitem %d part %d cost %d\n", i, j, cost);
            }
        }
    }


    public static void main(String[] args) {

        init();

        // l_orderkey == 0, o_orderkey == 0
        // lineitem 14, orders 11

        // o_custkey == 1, c_custkey == 0
        // orders 11, customer 9

        LineitemJoinOrders();
        //OrdersJoinCustomer();

        //LineitemJoinOrdersWithQ();

        //LineitemJoinPart();

        //LineitemJoinOrdersWithQ10();

        System.out.println("Done!");
    }
}