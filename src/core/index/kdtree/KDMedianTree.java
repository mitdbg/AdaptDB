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
            dimensionTypes = k.detectTypes(true);
            numDimensions = dimensionTypes.length;
        }
        this.buckets.get(0).insert(k);
    }

    @Override
    public void initProbe() {
        int numSplits = (int) (Math.log(maxBuckets) / Math.log(2)) + 1;
        System.out.println("Depth is " + numSplits);
        for (int i = 0; i < numSplits; i++) {
            splitTree(buckets, i % numDimensions);
        }
    }

    private void splitTree(List<CartilageIndexKeySet> buckets, int dim) {
        int n = buckets.size();
        for (int i = 0; i < n; i++) {
            CartilageIndexKeySet keys = buckets.remove(0);
            if (keys.size() > 1) {
                Pair<CartilageIndexKeySet, CartilageIndexKeySet> halves = keys.sortAndSplit(dim);
                buckets.add(halves.first);
                buckets.add(halves.second);
                super.insert(halves.second.iterator().next());
            }
        }
    }
}
