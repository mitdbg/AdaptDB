package core.index.kdtree;

import java.util.Arrays;
import java.util.List;

import core.index.MDIndex;
import core.index.key.CartilageIndexKey2;
import core.index.key.MDIndexKey;
import core.utils.SchemaUtils.TYPE;

public class KDDTree implements MDIndex {

    private int numDimensions;
    private TYPE[] dimensionTypes;
    private int maxBuckets;
    private KDNode root;

	@Override
	public void initBuild(int buckets) {
        this.maxBuckets = buckets;
        this.root = new KDNode();
    }

	@Override
	public void insert(MDIndexKey key) {
        if (root.getNumBuckets() >= maxBuckets) {
            return;
        }
        CartilageIndexKey2 k = (CartilageIndexKey2)key;

        if (dimensionTypes == null) {
            dimensionTypes = k.detectTypes();
            numDimensions = dimensionTypes.length;
        }

        KDNode newNode = this.root.insert(key);
        int nextDimension = (newNode.getDimension() + 1) % numDimensions;
        newNode.setValues(nextDimension, dimensionTypes[nextDimension], key);
	}

	@Override
	public void bulkLoad(MDIndexKey[] keys) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void initProbe() {
        System.out.println("OK");
	}
	
	@Override
	public int getBucketId(MDIndexKey key) {
		return this.root.getBucketId(key, 1);
	}

	@Override
	public Bucket search(MDIndexKey key) {
		// TODO Auto-generated method stub
		// very similar to the above method!
		return null;
	}

	@Override
	public List<Bucket> range(MDIndexKey low, MDIndexKey high) {
		// TODO Auto-generated method stub
		return this.root.rangeSearch(low, high);
	}

	@Override
	public byte[] marshall() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void unmarshall(byte[] bytes) {
		// TODO Auto-generated method stub
		
	}
}
