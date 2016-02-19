package core.adapt.opt;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

import core.adapt.JoinQuery;
import core.adapt.spark.join.SparkJoinQueryConf;
import core.common.globals.TableInfo;
import core.common.index.JRNode;
import core.common.index.JoinRobustTree;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import core.adapt.Predicate;
import core.adapt.AccessMethod.PartitionSplit;
import core.adapt.iterator.PartitionIterator;
import core.adapt.iterator.PostFilterIterator;
import core.adapt.iterator.RepartitionIterator;


import core.common.index.MDIndex.Bucket;
import core.common.key.ParsedTupleList;
import core.utils.ConfUtils;
import core.utils.HDFSUtils;
import core.utils.Pair;
import core.utils.TypeUtils;
import core.utils.TypeUtils.TYPE;
/**
 * Created by ylu on 1/21/16.
 */


/**
 * Optimizer creates the execution plans for the queries.
 * It uses the incoming query predicates as hints for what should be added
 * into the partitioning tree. If we find a plan which has benefit >
 * cost, we do the repartitioning. Else we just do a scan.
 *
 * @author yilu
 */
public class JoinOptimizer {
    private static final double WRITE_MULTIPLIER = 1.5;

    private JoinRobustTree rt;

    // Properties extracted from ConfUtils
    private String workingDir;
    private String hadoopHome;
    private short fileReplicationFactor;

    private List<JoinQuery> queryWindow = new ArrayList<JoinQuery>();

    public JoinOptimizer(SparkJoinQueryConf cfg) {
        // Working Directory for the Optimizer.
        // Each table is a folder under this directory.
        this.workingDir = cfg.getWorkingDir();
        this.hadoopHome = cfg.getHadoopHome();
        this.fileReplicationFactor = cfg.getHDFSReplicationFactor();
    }

    public JoinOptimizer(ConfUtils cfg) {
        this.workingDir = cfg.getHDFS_WORKING_DIR();
        this.hadoopHome = cfg.getHADOOP_HOME();
        this.fileReplicationFactor = cfg.getHDFS_REPLICATION_FACTOR();
    }

    public void loadIndex(TableInfo tableInfo) {
        FileSystem fs = HDFSUtils.getFSByHadoopHome(hadoopHome);
        String tableDir = this.workingDir + "/" + tableInfo.tableName;
        String pathToIndex = tableDir + "/index";
        String pathToSample = tableDir + "/sample";

        System.out.println("Load index: " + pathToIndex);

        byte[] indexBytes = HDFSUtils.readFile(fs, pathToIndex);
        this.rt = new JoinRobustTree(tableInfo);
        this.rt.unmarshall(indexBytes);

        byte[] sampleBytes = HDFSUtils.readFile(fs, pathToSample);
        this.rt.loadSample(tableInfo, sampleBytes);
    }

    public void printTree() {
        rt.printTree();
    }

    public JoinRobustTree getIndex() {
        return rt;
    }

    public int[] getBidFromRNodes(List<JRNode> nodes) {
        int[] bids = new int[nodes.size()];
        Iterator<JRNode> it = nodes.iterator();
        for (int i = 0; i < bids.length; i++) {
            bids[i] = it.next().bucket.getBucketId();
        }
        return bids;
    }

    public PartitionSplit[] buildAccessPlan(JoinQuery fq) {
        List<JRNode> nodes = this.rt.getRoot().search(fq.getPredicates());
        PartitionIterator pi = new PostFilterIterator(fq.castToQuery());
        int[] bids = this.getBidFromRNodes(nodes);

        PartitionSplit psplit = new PartitionSplit(bids, pi);
        PartitionSplit[] ps = new PartitionSplit[1];
        ps[0] = psplit;
        return ps;
    }


    public PartitionSplit[] buildPlan(JoinQuery q) {
        this.queryWindow.add(q);

        Predicate[] ps = q.getPredicates();
        LinkedList<Predicate> choices = new LinkedList<Predicate>();

        // Initialize the set of choices for predicates.
        for (int i = 0; i < ps.length; i++) {
            choices.add(ps[i]);
        }

        adjustJoinRobustTree(choices, q);

        JRNode root = rt.getRoot();

        System.out.println("plan.cost: " + root.cost + " plan.benefit: "
                + root.benefit);

        rt.printTree(); /////////

        boolean updated = rt.isUpdated();

        System.out.println("Updated?: " + updated);

        PartitionSplit[] psplits;

        if (updated) {
            psplits = this.getPartitionSplits(q);
        } else {
            psplits = this.buildAccessPlan(q);
        }

        this.persistQueryToDisk(q);
        if (updated) {
            System.out.println("INFO: persist index to disk");
            updateBucketIds(rt.getRoot());
            persistIndexToDisk();
            for (int i = 0; i < psplits.length; i++) {
                if (psplits[i].getIterator().getClass() == RepartitionIterator.class) {
                    psplits[i] = new PartitionSplit(
                            psplits[i].getPartitions(),
                            new RepartitionIterator(q.castToQuery()));
                }
            }
        } else {
            System.out.println("INFO: No index update");
        }

        return psplits;
    }

    private void adjustJoinRobustTree(List<Predicate> choices, JoinQuery q) {
        Predicate[] ps = q.getPredicates();
        for (Predicate p : ps) {
            adjustJoinRobustTreeForPredicate(rt.getRoot(), p, ps);
        }
        //adjustJoinRobustTreeForJoinAttribute(rt.getRoot(), q);

    }


    private void getPartitionSplits_helper(JRNode node, List<Integer> unmodifiedBuckets, List<Integer> modifiedBuckets) {
        if (node.bucket != null) {
            if (node.fullAccessed && node.updated == false) {
                unmodifiedBuckets.add(node.bucket.getBucketId());
            } else if (node.fullAccessed && node.updated == true) {
                modifiedBuckets.add(node.bucket.getBucketId());
            }
        } else {
            getPartitionSplits_helper(node.leftChild, unmodifiedBuckets, modifiedBuckets);
            getPartitionSplits_helper(node.rightChild, unmodifiedBuckets, modifiedBuckets);
        }
    }

    private PartitionSplit[] getPartitionSplits(JoinQuery q) {
        List<Integer> unmodifiedBuckets = new ArrayList<Integer>();
        List<Integer> modifiedBuckets = new ArrayList<Integer>();
        JRNode root = rt.getRoot();

        getPartitionSplits_helper(root, unmodifiedBuckets, modifiedBuckets);

        PartitionSplit[] splits = new PartitionSplit[2];
        int[] unmodifiedBucketsIds = new int[unmodifiedBuckets.size()];
        for (int i = 0; i < unmodifiedBucketsIds.length; i++) {
            unmodifiedBucketsIds[i] = unmodifiedBuckets.get(i);
        }
        PartitionIterator pi = new PostFilterIterator(q.castToQuery());
        splits[0] = new PartitionSplit(unmodifiedBucketsIds, pi);


        int[] modifiedBucketsIds = new int[modifiedBuckets.size()];
        for (int i = 0; i < modifiedBucketsIds.length; i++) {
            modifiedBucketsIds[i] = modifiedBuckets.get(i);
        }
        PartitionIterator pj = new RepartitionIterator(q.castToQuery());
        splits[1] = new PartitionSplit(modifiedBucketsIds, pj);

        return splits;
    }

    /**
     * Puts the new estimated number of tuples in each bucket after change
     *
     * @param changed
     */
    private void populateBucketEstimates(JRNode changed) {
        ParsedTupleList collector = null;
        double numTuples = 0;
        int numSamples = 0;

        LinkedList<JRNode> stack = new LinkedList<JRNode>();
        stack.add(changed);

        while (stack.size() > 0) {
            JRNode n = stack.removeLast();
            if (n.bucket != null) {
                ParsedTupleList bucketSample = n.bucket.getSample();
                if (collector == null) {
                    collector = new ParsedTupleList(bucketSample.getTypes());
                }

                numSamples += bucketSample.getValues().size();
                numTuples += n.bucket.getEstimatedNumTuples();
                collector.addValues(bucketSample.getValues());
            } else {
                stack.add(n.rightChild);
                stack.add(n.leftChild);
            }
        }

        populateBucketEstimates(changed, collector, numTuples / numSamples);
    }

    private void populateBucketEstimates(JRNode n, ParsedTupleList sample,
                                         double scaleFactor) {
        if (n.bucket != null) {
            n.bucket.setEstimatedNumTuples(sample.size() * scaleFactor);
        } else {
            // By sorting we avoid memory allocation
            // Will most probably be faster
            sample.sort(n.attribute);
            Pair<ParsedTupleList, ParsedTupleList> halves = sample
                    .splitAt(n.attribute, n.value);
            populateBucketEstimates(n.leftChild, halves.first, scaleFactor);
            populateBucketEstimates(n.rightChild, halves.second, scaleFactor);
        }
    }

    /**
     * Gives the number of tuples accessed
     *
     * @param changed
     * @return
     */
    private double getNumTuplesAccessed(JRNode changed) {
        // First traverse to parent to see if query accesses node
        // If yes, find the number of tuples accessed.
        double numTuples = 0;

        for (int i = queryWindow.size() - 1; i >= 0; i--) {
            JoinQuery q = queryWindow.get(i);
            numTuples += getNumTuplesAccessed(changed, q);
        }

        return numTuples;
    }

    static float getNumTuplesAccessed(JRNode changed, JoinQuery q) {
        // First traverse to parent to see if query accesses node
        // If yes, find the number of tuples accessed.
        Predicate[] ps = q.getPredicates();

        JRNode node = changed;
        boolean accessed = true;
        while (node.parent != null) {
            for (Predicate p : ps) {
                if (p.attribute == node.parent.attribute) {
                    if (node.parent.leftChild == node) {
                        switch (p.predtype) {
                            case EQ:
                            case GEQ:
                                if (TypeUtils.compareTo(p.value, node.parent.value,
                                        node.parent.type) > 0)
                                    accessed = false;
                                break;
                            case GT:
                                if (TypeUtils.compareTo(p.value, node.parent.value,
                                        node.parent.type) >= 0)
                                    accessed = false;
                                break;
                            default:
                                break;
                        }
                    } else {
                        switch (p.predtype) {
                            case EQ:
                            case LEQ:
                                if (TypeUtils.compareTo(p.value, node.parent.value,
                                        node.parent.type) <= 0)
                                    accessed = false;
                                break;
                            case LT:
                                if (TypeUtils.compareTo(p.value, node.parent.value,
                                        node.parent.type) < 0)
                                    accessed = false;
                                break;
                            default:
                                break;
                        }
                    }
                }

                if (!accessed)
                    break;
            }

            if (!accessed)
                break;
            node = node.parent;
        }

        List<JRNode> nodesAccessed = changed.search(ps);
        float tCount = 0;
        for (JRNode n : nodesAccessed) {
            tCount += n.bucket.getEstimatedNumTuples();
        }

        return tCount;
    }

    private void replaceInTree(JRNode old, JRNode r) {
        old.leftChild.parent = r;
        old.rightChild.parent = r;
        if (old.parent != null) {
            if (old.parent.rightChild == old) {
                old.parent.rightChild = r;
            } else {
                old.parent.leftChild = r;
            }
        }

        r.leftChild = old.leftChild;
        r.rightChild = old.rightChild;
        r.parent = old.parent;
    }


    private void updateBucketIds(JRNode node) {

        if(node.bucket != null){
            if(node.updated){
                node.bucket.updateId();
            }
            return;
        }
        updateBucketIds(node.leftChild);
        updateBucketIds(node.rightChild);
    }


    private boolean checkIfGoLeft(JRNode node, Predicate[] ps) {
        boolean goLeft = true;
        for (int i = 0; i < ps.length; i++) {
            Predicate pd = ps[i];
            if (pd.attribute == node.attribute) {
                switch (pd.predtype) {
                    case GEQ:
                        if (TypeUtils
                                .compareTo(pd.value, node.value, node.type) > 0)
                            goLeft = false;
                        break;
                    case LEQ:
                        break;
                    case GT:
                        if (TypeUtils
                                .compareTo(pd.value, node.value, node.type) >= 0)
                            goLeft = false;
                        break;
                    case LT:
                        break;
                    case EQ:
                        if (TypeUtils
                                .compareTo(pd.value, node.value, node.type) > 0)
                            goLeft = false;
                        break;
                }
            }
        }
        return goLeft;
    }

    private boolean checkIfGoRight(JRNode node, Predicate[] ps) {
        boolean goRight = true;
        for (int i = 0; i < ps.length; i++) {
            Predicate pd = ps[i];
            if (pd.attribute == node.attribute) {
                switch (pd.predtype) {
                    case GEQ:
                        break;
                    case LEQ:
                        if (TypeUtils
                                .compareTo(pd.value, node.value, node.type) <= 0)
                            goRight = false;
                        break;
                    case GT:
                        break;
                    case LT:
                        if (TypeUtils
                                .compareTo(pd.value, node.value, node.type) < 0)
                            goRight = false;
                        break;
                    case EQ:
                        if (TypeUtils
                                .compareTo(pd.value, node.value, node.type) <= 0)
                            goRight = false;
                        break;
                }
            }
        }
        return goRight;
    }

    private void partitionSubTreeByJoinAttribute(JRNode node, int joinAttribute, ParsedTupleList sample) {
        // grab the median, change the atrr and value on the node, then recurse.
        if (node.bucket != null) {
            node.updated = true;
            return;
        }

        sample.sort(joinAttribute);

        List<Object[]> values = sample.getValues();
        Object medianVal = values.get(values.size() / 2)[joinAttribute];

        node.attribute = joinAttribute;
        node.value = medianVal;
        node.type = sample.getTypes()[joinAttribute];

        Pair<ParsedTupleList, ParsedTupleList> halves = sample.splitByMedian(joinAttribute);
        partitionSubTreeByJoinAttribute(node.leftChild, joinAttribute, halves.first);
        partitionSubTreeByJoinAttribute(node.rightChild, joinAttribute, halves.second);
    }

    private void adjustJoinRobustTreeForJoinAttribute(JRNode node, JoinQuery q) {
        // attributes are from the queryWindow
        if (node.bucket != null) {
            // Leaf
            return;
        } else {
            // Check if both sides are accessed
            //boolean goLeft = checkIfGoLeft(node, ps);
            //boolean goRight = checkIfGoRight(node, ps);
            if (node.fullAccessed) {
                // maybe some other condition
                ParsedTupleList collector = null;
                // populate the sample

                LinkedList<JRNode> stack = new LinkedList<JRNode>();
                stack.add(node);

                while (stack.size() > 0) {
                    JRNode n = stack.removeLast();
                    if (n.bucket != null) {
                        ParsedTupleList bucketSample = n.bucket.getSample();
                        if (collector == null) {
                            collector = new ParsedTupleList(bucketSample.getTypes());
                        }
                        collector.addValues(bucketSample.getValues());
                    } else {
                        stack.add(n.rightChild);
                        stack.add(n.leftChild);
                    }
                }
                // partition the tree by join attributes

                partitionSubTreeByJoinAttribute(node, q.getJoinAttribute(), collector);

            } else {
                adjustJoinRobustTreeForJoinAttribute(node.leftChild, q);
                adjustJoinRobustTreeForJoinAttribute(node.rightChild, q);
            }
        }

    }


    private void adjustJoinRobustTreeForPredicate(JRNode node, Predicate choice, Predicate[] ps) {
        // Option Index
        // 1 => Replace
        // 2 => Swap down X

        if (node.bucket != null) {
            // Leaf
            node.fullAccessed = true;
            return;
        } else {
            Predicate p = choice;

            // Check if both sides are accessed
            boolean goLeft = checkIfGoLeft(node, ps);
            boolean goRight = checkIfGoRight(node, ps);

            if (goLeft) {
                adjustJoinRobustTreeForPredicate(node.leftChild, choice, ps);
            }

            if (goRight) {
                adjustJoinRobustTreeForPredicate(node.rightChild, choice, ps);
            }

            if (node.leftChild.fullAccessed && node.rightChild.fullAccessed) {
                node.fullAccessed = true;

                // When trying to replace by predicate;
                // Replace by testVal, not the actual predicate value
                Object testVal = p.getHelpfulCutpoint();

                // replace attribute by one in the predicate


                // If we traverse to root and see that there is no node with
                // cutoff point less than
                // that of predicate, we can do this

                double numAccessedOld = getNumTuplesAccessed(node);

                JRNode r = node.clone();
                r.attribute = p.attribute;
                r.type = p.type;
                r.value = testVal;
                replaceInTree(node, r);

                populateBucketEstimates(r);
                double numAcccessedNew = getNumTuplesAccessed(r);
                double benefit = numAccessedOld - numAcccessedNew;
                double cost = this.computeCost(r); // Note that buckets


                if (benefit < cost) {
                    // Restore ??
                    replaceInTree(r, node);
                    populateBucketEstimates(node);
                } else {
                    r.leftChild.updated = true;
                    r.rightChild.updated = true;
                }


                // Swap down the attribute and bring p above

                if (node.leftChild.bucket == null && node.rightChild.bucket == null &&
                        node.leftChild.attribute == node.rightChild.attribute &&
                        node.leftChild.value.equals(node.rightChild.value)) {

                    node.benefit = node.leftChild.benefit + node.rightChild.benefit;
                    node.cost = node.leftChild.cost + node.rightChild.cost;

                    int attribute = node.attribute;
                    Object value = node.value;
                    TYPE type = node.type;

                    // some condition here

                    node.attribute = node.leftChild.attribute;
                    node.value = node.leftChild.value;
                    node.type = node.leftChild.type;

                    node.leftChild.attribute = attribute;
                    node.leftChild.value = value;
                    node.leftChild.type = type;

                    node.rightChild.attribute = attribute;
                    node.rightChild.value = value;
                    node.rightChild.type = type;

                }

            }


            return;
        }
    }

    private double computeCost(JRNode r) {
        double numTuples = r.numTuplesInSubtree();
        return WRITE_MULTIPLIER * numTuples;
    }

    public void loadQueries(TableInfo tableInfo) {
        FileSystem fs = HDFSUtils.getFSByHadoopHome(hadoopHome);
        String pathToQueries = this.workingDir + "/" + tableInfo.tableName + "/queries";
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
                sc.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void persistQueryToDisk(JoinQuery q) {
        String pathToQueries = this.workingDir + "/" + q.getTable() + "/queries";
        FileSystem fs = HDFSUtils.getFSByHadoopHome(hadoopHome);
        HDFSUtils.safeCreateFile(fs, pathToQueries,
                this.fileReplicationFactor);
        HDFSUtils.appendLine(fs, pathToQueries, q.toString());
    }

    private void persistIndexToDisk() {
        String pathToIndex = this.workingDir + "/" + rt.tableInfo.tableName + "/index";
        FileSystem fs = HDFSUtils.getFSByHadoopHome(hadoopHome);
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
            HDFSUtils.safeCreateFile(fs, pathToIndex,
                    this.fileReplicationFactor);
        } catch (IOException e) {
            System.out.println("ERR: Writing Index failed: " + e.getMessage());
            e.printStackTrace();
        }

        byte[] indexBytes = this.rt.marshall();
        HDFSUtils.writeFile(fs,
                pathToIndex, this.fileReplicationFactor, indexBytes, 0,
                indexBytes.length, false);
    }
}
