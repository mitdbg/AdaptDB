package core.index.kdtree;

import core.index.MDIndex;
import core.index.key.CartilageIndexKey;
import core.index.key.CartilageIndexKeySet;
import core.index.key.MDIndexKey;
import core.utils.Pair;

import java.util.*;

/**
 * Created by qui on 2/19/15.
 */
public class KDMedianTree extends KDDTree implements MDIndex {

    private List<CartilageIndexKeySet> buckets;
    private double samplingRate;

    public KDMedianTree(double samplingRate) {
        this(samplingRate, null);
    }

    public KDMedianTree(double samplingRate, int[] attrs) {
        super(attrs);
        this.samplingRate = samplingRate;
    }

    @Override
    public MDIndex clone() { return new KDMedianTree(samplingRate, attrOrder); }

    @Override
    public void initBuild(int numBuckets) {
        int numLevels = (int) Math.ceil(Math.log(numBuckets) / Math.log(2));
        int numBucketsEven = (int) Math.pow(2, numLevels);
        super.initBuild(numBucketsEven);
        buckets = new ArrayList<CartilageIndexKeySet>();
        buckets.add(new CartilageIndexKeySet());
    }

    @Override
    public void insert(MDIndexKey key) {
        CartilageIndexKey k = (CartilageIndexKey) key;
        if (dimensionTypes == null) {
            initializeDimensions(k);
        }
        if (Math.random() < samplingRate) {
            this.buckets.get(0).insert(k);
        }
    }

    @Override
    public void initProbe() {
        int numSplits = (int) (Math.log(maxBuckets) / Math.log(2)) + 1;
        System.out.println("Depth is " + numSplits);
        int currentDim = -1;
        double[] allocations = new double[dimensionTypes.length];
        for (int i = 0; i < numSplits; i++) {
            int dimToSplit = nextAttrIdx.get(currentDim);
            int numNodesAdded = splitTree(buckets, dimToSplit);
            if (i < numSplits - 1) { // last level doesn't count towards allocation
                allocations[dimToSplit] += numNodesAdded / Math.pow(2, i - 1);
            }
            currentDim = dimToSplit;
        }
        System.out.println("Allocations: "+Arrays.toString(allocations));
    }

    private int splitTree(List<CartilageIndexKeySet> buckets, int dim) {
        int n = buckets.size();
        int numNodesAdded = 0;
        for (int i = 0; i < n; i++) {
            CartilageIndexKeySet keys = buckets.remove(0);
            if (keys.size() > 1) {
                Pair<CartilageIndexKeySet, CartilageIndexKeySet> halves = keys.sortAndSplit(dim);
                buckets.add(halves.first);
                buckets.add(halves.second);
                CartilageIndexKey key = halves.second.iterator().next();
                super.insert(key);
                numNodesAdded++;
            }
        }
        return numNodesAdded;
    }
}
