package core.index.kdtree;

import java.util.List;

import core.adapt.Predicate;
import core.index.MDIndex;
import core.index.key.CartilageIndexKey;
import core.index.key.MDIndexKey;
import core.utils.SchemaUtils.TYPE;

public class KDDTree implements MDIndex {

    int numDimensions;
    TYPE[] dimensionTypes;
    int maxBuckets;
    KDNode root;

    @Override
    public MDIndex clone() throws CloneNotSupportedException {
        return new KDDTree();
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
            dimensionTypes = k.detectTypes(true);
            numDimensions = dimensionTypes.length;
        }

        KDNode newNode = this.root.insert(key);
        int nextDimension = (newNode.getParentDimension() + 1) % numDimensions;
        newNode.setValues(nextDimension, dimensionTypes[nextDimension], key);
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
