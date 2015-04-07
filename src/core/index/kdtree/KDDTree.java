package core.index.kdtree;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import core.adapt.Predicate;
import core.index.MDIndex;
import core.index.key.CartilageIndexKey;
import core.index.key.MDIndexKey;
import core.utils.SchemaUtils.TYPE;

public class KDDTree implements MDIndex {

    TYPE[] dimensionTypes;
    int[] attrOrder;
    Map<Integer, Integer> nextAttrIdx = new HashMap<Integer, Integer>();
    int maxBuckets;
    KDNode root;

    public KDDTree() {
    }

    public KDDTree(int[] attrOrder) {
        this.attrOrder = Arrays.copyOf(attrOrder, attrOrder.length);
    }

    @Override
    public MDIndex clone() throws CloneNotSupportedException {
        return new KDDTree(attrOrder);
    }

	public void initBuild(int buckets) {
        this.maxBuckets = buckets;
        this.root = new KDNode();
    }

	public void insert(MDIndexKey key) {
        if (root.getNumBuckets() >= maxBuckets) {
            return;
        }
        CartilageIndexKey k = (CartilageIndexKey)key;

        if (dimensionTypes == null) {
            initializeDimensions(k);
        }

        KDNode newNode = this.root.insert(key);
        int nextDimension = nextAttrIdx.get((newNode.getParentDimension()));
        newNode.setValues(nextDimension, dimensionTypes[nextDimension], key);
	}

    void initializeDimensions(CartilageIndexKey key) {
        dimensionTypes = key.detectTypes(true);
        int numDimensions = dimensionTypes.length;
        int[] keys = key.getKeys();

        if (attrOrder == null) {
            nextAttrIdx.put(-1, 0);
            for (int i = 0; i < keys.length; i++) {
                nextAttrIdx.put(i, (i+1) % numDimensions);
            }
        } else {
            Map<Integer, Integer> keyToIndex = new HashMap<Integer, Integer>();
            for (int i = 0; i < keys.length; i++) {
                keyToIndex.put(keys[i], i);
            }
            nextAttrIdx.put(-1, keyToIndex.get(attrOrder[0]));
            for (int i = 0; i < attrOrder.length; i++) {
                nextAttrIdx.put(keyToIndex.get(attrOrder[i]), keyToIndex.get(attrOrder[(i+1)%numDimensions]));
            }
        }
    }

	public void bulkLoad(MDIndexKey[] keys) {
		// TODO Auto-generated method stub

	}

	public void initProbe() {
        System.out.println("initProbe OK");
	}

	public Object getBucketId(MDIndexKey key) {
        return Integer.toString(this.root.getBucketId(key, 1));
	}

	public List<Bucket> search(Predicate[] predicates) {
		// TODO Auto-generated method stub
		return null;
	}

	public List<Bucket> range(MDIndexKey low, MDIndexKey high) {
		// TODO Auto-generated method stub
		return this.root.rangeSearch(low, high);
	}

	public byte[] marshall() {
		// TODO Auto-generated method stub
		return null;
	}

	public void unmarshall(byte[] bytes) {
		// TODO Auto-generated method stub

	}
}
