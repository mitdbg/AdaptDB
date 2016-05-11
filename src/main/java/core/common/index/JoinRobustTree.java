package core.common.index;


import java.util.*;

import core.adapt.AccessMethod.PartitionSplit;
import core.adapt.JoinQuery;
import core.adapt.Predicate;
import core.adapt.iterator.JoinRepartitionIterator;
import core.adapt.iterator.PartitionIterator;
import core.common.globals.TableInfo;
import core.common.key.ParsedTupleList;
import core.common.key.RawIndexKey;
import core.utils.Pair;
import core.utils.TypeUtils.TYPE;

/**
 * Created by ylu on 1/21/16.
 */


public class JoinRobustTree implements MDIndex {
    public TableInfo tableInfo;

    public int maxBuckets;
    public int numAttributes;
    public int joinAttributeDepth;
    public ParsedTupleList sample;

    public TYPE[] dimensionTypes;
    JRNode root;

    public static Random randGenerator = new Random();

    public JoinRobustTree(TableInfo tableInfo) {
        this.root = new JRNode();
        this.tableInfo = tableInfo;
    }

    public JoinRobustTree() {
    }

    public void setMaxBuckets(int maxBuckets) {
        this.maxBuckets = maxBuckets;
    }

    public int getMaxBuckets() {
        return maxBuckets;
    }

    @Override
    public MDIndex clone() throws CloneNotSupportedException {
        throw new CloneNotSupportedException();
    }

    public JRNode getRoot() {
        return root;
    }

    // Only used for testing
    public void setRoot(JRNode root) {
        this.root = root;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof JoinRobustTree) {
            JoinRobustTree rhs = (JoinRobustTree) obj;
            boolean allGood = true;
            allGood &= rhs.numAttributes == this.numAttributes;
            allGood &= rhs.maxBuckets == this.maxBuckets;
            allGood &= rhs.dimensionTypes.length == this.dimensionTypes.length;
            if (!allGood)
                return false;

            for (int i = 0; i < this.dimensionTypes.length; i++) {
                allGood &= this.dimensionTypes[i] == rhs.dimensionTypes[i];
            }
            if (!allGood)
                return false;

            allGood = this.root == rhs.root;
            return allGood;
        }
        return false;
    }

    public PartitionSplit delete(long delteSize, boolean deleteAll,JoinQuery query, int indexPartition, Map<Integer, Long> partitionIdSizeMap) {

        LinkedList<JRNode> nodeQueue = new LinkedList<JRNode>();
        LinkedList<JRNode> deleteNodes = new LinkedList<JRNode>();

        List<Integer> bucket_ids = new ArrayList<Integer>();


        PartitionIterator pi = new JoinRepartitionIterator(query.castToQuery(), indexPartition);

        // Initialize root with attribute 0
        nodeQueue.add(this.getRoot());

        while (nodeQueue.size() > 0) {
            JRNode t = nodeQueue.pollFirst();

            if (t.bucket != null){
                continue;
            }

            deleteNodes.add(t);

            nodeQueue.add(t.leftChild);
            nodeQueue.add(t.rightChild);
        }

        // reverse deleteNodes

        Collections.reverse(deleteNodes);


        if (deleteAll){

            int[] buckets = getAllBuckets();
            PartitionSplit psplit = new PartitionSplit(buckets, pi);
            return psplit;
        } else {
            for (JRNode node: deleteNodes){

                if (delteSize <= 0) break;

                if (node.leftChild.bucket == null || node.rightChild.bucket == null){
                    throw new RuntimeException();
                }

                if (node == this.getRoot()){

                    int lbid = node.leftChild.bucket.getBucketId();
                    int rbid = node.rightChild.bucket.getBucketId();
                    if (partitionIdSizeMap.containsKey(lbid))
                    {
                        delteSize -= partitionIdSizeMap.get(lbid);
                    }
                    if (partitionIdSizeMap.containsKey(rbid))
                    {
                        delteSize -= partitionIdSizeMap.get(rbid);
                    }

                    bucket_ids.add(lbid);
                    bucket_ids.add(rbid);

                    this.root = null;

                } else{
                    // delete left child
                    JRNode parent = node.parent;

                    if (parent.rightChild == node){
                        parent.rightChild = node.rightChild;
                    } else {
                        parent.leftChild = node.rightChild;
                    }

                    int bid = node.leftChild.bucket.getBucketId();
                    if (partitionIdSizeMap.containsKey(bid))
                    {
                        delteSize -= partitionIdSizeMap.get(bid);
                    }
                    bucket_ids.add(bid);

                }
            }

            int[] buckets = new int[bucket_ids.size()];

            for(int i =0 ;i < buckets.length; i ++){
                buckets[i] = bucket_ids.get(i);
            }

            PartitionSplit psplit = new PartitionSplit(buckets, pi);
            return psplit;

        }

    }

    public class Task {
        JRNode node;
        int depth;
        ParsedTupleList sample;
    }

    /**
     * Created the tree based on the histograms
     */
    @Override
    public void initProbe() {
        System.out.println("method not implemented!");
    }


    @Override
    public void initProbe(int joinAttribute) {
        System.out.println(this.sample.size() + " keys inserted");

        // Computes log(this.maxBuckets)
        int maxDepth = 31 - Integer.numberOfLeadingZeros(this.maxBuckets);
        double allocationPerAttribute = RobustTree.nthroot(
                this.numAttributes, this.maxBuckets);
        System.out.println("Max allocation: " + allocationPerAttribute);

        double[] allocations = new double[this.numAttributes];
        for (int i = 0; i < this.numAttributes; i++) {
            allocations[i] = allocationPerAttribute;
        }

        /**
         * Do a level-order traversal
         */
        LinkedList<Task> nodeQueue = new LinkedList<Task>();
        // Initialize root with attribute 0
        Task initialTask = new Task();
        initialTask.node = root;
        initialTask.sample = this.sample;
        initialTask.depth = 1;
        nodeQueue.add(initialTask);

        while (nodeQueue.size() > 0) {
            Task t = nodeQueue.pollFirst();


            if (t.depth > maxDepth) {
                Bucket b = new Bucket();
                b.setSample(t.sample);
                t.node.bucket = b;
                continue;
            }

            int dim = -1;
            Pair<ParsedTupleList, ParsedTupleList> halves = null;

            if (t.depth <= this.joinAttributeDepth) {
                dim = joinAttribute;
                allocations[dim] -= 2.0 / Math.pow(2, t.depth - 1);
                halves = t.sample.sortAndSplit(dim);
            } else if (t.depth <= maxDepth) {

                boolean[] validDims = new boolean[numAttributes];
                Arrays.fill(validDims, true);

                for (int i = 0; i < numAttributes; i++) {
                    int testDim = getLeastAllocated(allocations, validDims);
                    halves = t.sample.sortAndSplit(testDim);
                    if (halves.first.size() > 0 && halves.second.size() > 0) {
                        dim = testDim;
                        allocations[dim] -= 2.0 / Math.pow(2, t.depth - 1);
                        break;
                    } else {
                        validDims[testDim] = false;
                        System.err.println("WARN: Skipping attribute " + testDim);
                    }
                }

            }

            if (dim == -1) {
                System.err.println("ERR: No attribute to partition on");
                Bucket b = new Bucket();
                b.setSample(t.sample);
                t.node.bucket = b;
            } else {
                t.node.attribute = dim;
                t.node.type = this.dimensionTypes[dim];
                t.node.value = halves.first.getLast(dim); // Need to traverse up for range.

                t.node.leftChild = new JRNode();
                t.node.leftChild.parent = t.node;
                Task tl = new Task();
                tl.node = t.node.leftChild;
                tl.depth = t.depth + 1;
                tl.sample = halves.first;
                nodeQueue.add(tl);

                t.node.rightChild = new JRNode();
                t.node.rightChild.parent = t.node;
                Task tr = new Task();
                tr.node = t.node.rightChild;
                tr.depth = t.depth + 1;
                tr.sample = halves.second;
                nodeQueue.add(tr);
            }
        }

        System.out.println("Final Allocations: " + Arrays.toString(allocations));
    }

    public void initProbe(JoinQuery q) {
        System.out.println(this.sample.size() + " keys inserted");
        randGenerator.setSeed(0);

        int joinAttribute = q.getJoinAttribute();

        // Computes log(this.maxBuckets)
        int maxDepth = 31 - Integer.numberOfLeadingZeros(this.maxBuckets);
        double allocationPerAttribute = Math.pow(this.maxBuckets, 1.0 / this.numAttributes);

        double[] allocations = new double[this.numAttributes];
        for (int i = 0; i < this.numAttributes; i++) {
            allocations[i] = allocationPerAttribute;
        }

        /**
         * Do a level-order traversal
         */
        LinkedList<Task> nodeQueue = new LinkedList<Task>();
        // Initialize root with attribute 0
        Task initialTask = new Task();
        initialTask.node = root;
        initialTask.sample = this.sample;
        initialTask.depth = 1;
        nodeQueue.add(initialTask);

        while (nodeQueue.size() > 0) {
            Task t = nodeQueue.pollFirst();


            if (t.depth > maxDepth) {
                Bucket b = new Bucket();
                b.setSample(t.sample);
                t.node.bucket = b;
                continue;
            }

            int dim = -1;
            Pair<ParsedTupleList, ParsedTupleList> halves = null;

            if (t.depth <= this.joinAttributeDepth) {
                dim = joinAttribute;
                allocations[dim] -= 2.0 / Math.pow(2, t.depth - 1);
                halves = t.sample.sortAndSplit(dim);
            } else if (t.depth <= maxDepth) {

                // use attributes from the query

                int numAttributes = this.numAttributes;
                boolean[] validDims = new boolean[numAttributes];
                Arrays.fill(validDims, false);

                Predicate[] ps = q.getPredicates();

                int numPredicates = 0;

                for (int i = 0; i < ps.length; i++) {
                    if (validDims[ps[i].attribute] == false) {
                        validDims[ps[i].attribute] = true;
                        numPredicates++;
                    }

                }

                dim = -1;

                for (int i = 0; i < numPredicates; i++) {
                    int testDim = getLeastAllocated(allocations, validDims);
                    halves = t.sample.sortAndSplit(testDim);
                    if (halves.first.size() > 0 && halves.second.size() > 0) {
                        dim = testDim;
                        allocations[dim] -= 2.0 / Math.pow(2, t.depth - 1);
                        break;
                    } else {
                        validDims[testDim] = false;
                    }
                }

                numPredicates = this.numAttributes;

                if (dim == -1) {
                    Arrays.fill(validDims, true);
                    for (int i = 0; i < numAttributes; i++) {
                        int testDim = getLeastAllocated(allocations, validDims);
                        halves = t.sample.sortAndSplit(testDim);
                        if (halves.first.size() > 0 && halves.second.size() > 0) {
                            dim = testDim;
                            allocations[dim] -= 2.0 / Math.pow(2, t.depth - 1);
                            break;
                        } else {
                            validDims[testDim] = false;
                            //System.err.println("WARN: Skipping attribute " + testDim);
                        }
                    }
                }

            }

            if (dim == -1) {
                System.err.println("ERR: No attribute to partition on");
                Bucket b = new Bucket();
                b.setSample(t.sample);
                t.node.bucket = b;
            } else {
                t.node.attribute = dim;
                t.node.type = this.dimensionTypes[dim];
                t.node.value = halves.first.getLast(dim); // Need to traverse up for range.

                t.node.leftChild = new JRNode();
                t.node.leftChild.parent = t.node;
                Task tl = new Task();
                tl.node = t.node.leftChild;
                tl.depth = t.depth + 1;
                tl.sample = halves.first;
                nodeQueue.add(tl);

                t.node.rightChild = new JRNode();
                t.node.rightChild.parent = t.node;
                Task tr = new Task();
                tr.node = t.node.rightChild;
                tr.depth = t.depth + 1;
                tr.sample = halves.second;
                nodeQueue.add(tr);
            }
        }

        System.out.println("Final Allocations: " + Arrays.toString(allocations));
    }

    /**
     * Return the dimension which has the maximum allocation unfulfilled
     */
    public static int getLeastAllocated(double[] allocations, boolean[] validDims) {

        boolean validFound = false;
        double minAlloc = 0;

        ArrayList<Integer> leastAllocated = new ArrayList<Integer>();

        for (int i = 0; i < allocations.length; i++) {
            if (validDims[i] == false) {
                continue;
            }
            if (validFound == false) {
                validFound = true;
                minAlloc = allocations[i];
                leastAllocated.add(i);
            } else {
                if (allocations[i] > minAlloc) {
                    minAlloc = allocations[i];
                    leastAllocated.clear();
                    leastAllocated.add(i);
                } else if (allocations[i] == minAlloc) {
                    leastAllocated.add(i);
                }
            }
        }

        if (leastAllocated.size() == 1) {
            return leastAllocated.get(0);
        } else {
            int r = randGenerator.nextInt(leastAllocated.size());
            return leastAllocated.get(r);
        }
    }

    /**
     * Used in the 2nd phase of upfront to assign each tuple to the right
     */
    @Override
    public Object getBucketId(RawIndexKey key) {
        return root.getBucketId(key);
    }


    private void getAllBucketsHelper(JRNode node, ArrayList<Integer> ids) {
        if (node.bucket != null) {
            ids.add(node.bucket.getBucketId());
            return;
        }
        getAllBucketsHelper(node.leftChild, ids);
        getAllBucketsHelper(node.rightChild, ids);
    }

    public int[] getAllBuckets() {
        ArrayList<Integer> ids = new ArrayList<Integer>();
        getAllBucketsHelper(root, ids);
        int[] buckets = new int[ids.size()];
        for (int i = 0; i < buckets.length; i++) {
            buckets[i] = ids.get(i);
        }
        return buckets;
    }

    /***************************************************
     * **************** RUNTIME METHODS *****************
     ***************************************************/

    public List<JRNode> getMatchingBuckets(Predicate[] predicates) {
        List<JRNode> results = root.search(predicates);
        return results;
    }

    /**
     * Serializes the index to string Very brittle - Consider rewriting
     */
    @Override
    public byte[] marshall() {
        // JVM optimizes shit so no need to use string builder / buffer
        // Format:
        // maxBuckets, numAttributes
        // types
        // nodes in pre-order

        String robustTree = "";
        robustTree += String.format("%d %d %d\n", this.maxBuckets,
                this.numAttributes, this.joinAttributeDepth);

        String types = "";
        for (int i = 0; i < this.numAttributes; i++) {
            types += this.dimensionTypes[i].toString() + " ";
        }
        types += "\n";
        robustTree += types;

        robustTree += this.root.marshall();

        return robustTree.getBytes();
    }

    @Override
    public void unmarshall(byte[] bytes) {
        String tree = new String(bytes);
        Scanner sc = new Scanner(tree);
        this.maxBuckets = sc.nextInt();
        this.numAttributes = sc.nextInt();
        this.joinAttributeDepth = sc.nextInt();

        this.dimensionTypes = new TYPE[this.numAttributes];
        for (int i = 0; i < this.numAttributes; i++) {
            this.dimensionTypes[i] = TYPE.valueOf(sc.next());
        }

        this.root = new JRNode();
        this.root.parseNode(sc);
    }

    public void loadSample(TableInfo tableInfo, byte[] bytes) {
        this.sample = new ParsedTupleList(tableInfo.getTypeArray());
        this.sample.unmarshall(bytes, tableInfo.delimiter);
        this.dimensionTypes = this.sample.getTypes();
        this.numAttributes = this.dimensionTypes.length;
        this.initializeBucketSamplesAndCounts(this.root, this.sample, this.sample.size(), tableInfo.numTuples);
    }

    public void loadSample(ParsedTupleList sample) {
        this.sample = sample;
        this.dimensionTypes = this.sample.getTypes();
        this.numAttributes = this.dimensionTypes.length;
    }

    public void initializeBucketSamplesAndCounts(JRNode n,
                                                 ParsedTupleList sample, final double totalSamples,
                                                 final double totalTuples) {

        if (n.bucket != null) {
            long sampleSize = sample.size();
            double numTuples = (sampleSize * totalTuples) / totalSamples;
            n.bucket.setSample(sample);
            n.bucket.setEstimatedNumTuples(numTuples);
        } else {
            // By sorting we avoid memory allocation
            // Will most probably be faster
            sample.sort(n.attribute);
            Pair<ParsedTupleList, ParsedTupleList> halves = sample
                    .splitAt(n.attribute, n.value);
            initializeBucketSamplesAndCounts(n.leftChild, halves.first,
                    totalSamples, totalTuples);
            initializeBucketSamplesAndCounts(n.rightChild, halves.second,
                    totalSamples, totalTuples);
        }
    }

    /**
     * Prints the tree created. Call only after initProbe is done.
     */
    public void printTree() {
        printNode(root);
    }

    public static void printNode(JRNode node) {
        if (node.bucket != null) {
            System.out.println("B " + node.bucket.getBucketId());
        } else {
            /*
            System.out.format("Node: %d %s { ", node.attribute,
                    node.value.toString());
            printNode(node.leftChild);
            System.out.print(" }{ ");
            printNode(node.rightChild);
            System.out.print(" }");
            */
            printNode(node.leftChild);
            printNode(node.rightChild);
        }
    }

    public Map<Integer, BucketInfo> getBucketRanges(int attribute) {
        return getBucketRangeSubtree(this.getRoot(), attribute);
    }

    private static Map<Integer, BucketInfo> getBucketRangeSubtree(JRNode root,
                                                                  int attribute) {
        if (root.bucket != null) {
            Map<Integer, BucketInfo> bucketRanges = new HashMap<Integer, BucketInfo>();
            if (root.rangesByAttribute.get(attribute) != null) {
                bucketRanges.put(root.bucket.getBucketId(),
                        root.rangesByAttribute.get(attribute));
            }
            return bucketRanges;
        } else {
            Map<Integer, BucketInfo> bucketRanges = getBucketRangeSubtree(
                    root.leftChild, attribute);
            bucketRanges.putAll(getBucketRangeSubtree(root.rightChild,
                    attribute));
            return bucketRanges;
        }
    }

    private void getAllocations_helper(JRNode node, int depth, double[] allocArray) {
        if (node.bucket != null) {
            return;
        }
        allocArray[node.attribute] += 2.0 / Math.pow(2, depth);
        getAllocations_helper(node.leftChild, depth + 1, allocArray);
        getAllocations_helper(node.rightChild, depth + 1, allocArray);
    }

    public double[] getAllocations() {
        double[] allocArray = new double[numAttributes];
        Arrays.fill(allocArray, 0);
        getAllocations_helper(root, 0, allocArray);
        return allocArray;
    }

    private void clearFlags_helper(JRNode node) {
        if (node.bucket != null) {
            return;
        }
        node.updated = false;
        node.fullAccessed = false;
        clearFlags_helper(node.leftChild);
        clearFlags_helper(node.rightChild);
    }

    public void clearFlags() {
        clearFlags_helper(root);
    }

    private boolean isUpdated_helper(JRNode node) {
        if (node.bucket != null) {
            return node.updated;
        }
        if (node.updated) {
            return true;
        }
        if (isUpdated_helper(node.leftChild)) {
            return true;
        } else {
            return isUpdated_helper(node.rightChild);
        }
    }

    public boolean isUpdated() {
        return isUpdated_helper(root);
    }
}
