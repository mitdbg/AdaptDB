package core.index.robusttree;

import java.util.List;

import org.apache.hadoop.hive.ql.udf.generic.NumericHistogram;

import core.index.MDIndex;
import core.index.key.CartilageIndexKey;
import core.index.key.MDIndexKey;
import core.utils.SchemaUtils.TYPE;

public class RobustTree implements MDIndex {
	int maxBuckets;
	int numDimensions;
	NumericHistogram[] histograms;
	TYPE[] dimensionTypes;
	RNode root;

	// Access costs of each attribute
	// Used only for the initial run
	float[] costs;

	@Override
	public MDIndex clone() throws CloneNotSupportedException {
		// TODO Auto-generated method stub
		return null;
	}

	public void initBuild(int buckets) {
        this.maxBuckets = buckets;
        this.numDimensions = 5;
        this.histograms = new NumericHistogram[this.numDimensions];

        for (int i=0; i<numDimensions; i++) {
        	this.histograms[i].allocate(10);
        }

        float[] costs = new float[this.numDimensions];
		for (int i=0; i<this.numDimensions; i++) {
			this.costs[i] = totalBuckets;
		}

		root = new RNode();
	}

	public void insert(MDIndexKey key) {
        if (dimensionTypes == null) {
            CartilageIndexKey k = (CartilageIndexKey)key;

        	this.dimensionTypes = k.detectTypes(true);
            this.numDimensions = dimensionTypes.length;
        }

        for (int i=0; i<numDimensions; i++) {
    		histograms[i].add(key.getDoubleAttribute(i));
        }
	}

	public void bulkLoad(MDIndexKey[] keys) {
		for (int i=0; i<keys.length; i++) {
			this.insert(keys[i]);
		}
	}


	/**
	 * Created the tree based on the histograms
	 */
	public void initProbe() {
		root.dimension = 0;
		root.type = this.dimensionTypes[0];
		root.value = this.histograms[0].quantile(0.5); // Get median

		this.costs[0] = getAccessCost(0);

		int depth = 31 - Integer.numberOfLeadingZeros(this.maxBuckets); // Computes log(this.maxBuckets)
		this.createTree(root, depth);
	}

	public void createTree(RNode node, int depth) {
		if (depth > 0) {
			int dim = getLeastEffective();
			node.dimension = dim;
			node.type = this.dimensionTypes[dim];
			node.value = 0; // Need to traverse up for range

			//
			float cost = this.getAccessCost(dim);
			this.costs[dim] = cost;

			node.leftChild = new RNode();
			this.createTree(node.leftChild, depth - 1);

			node.rightChild = new RNode();
			this.createTree(node.rightChild, depth - 1);
		} else {
			Bucket b = new Bucket();
			node.bucket = b;
		}
	}

	/**
	 * Finds the access cost of an attribute based on the tree
	 * @param dimNo
	 * @return
	 */
	public float getAccessCost(int dimNo) {

		return 0;
	}


	public int getLeastEffective() {
		int index = 0;
		float effectiveness = this.costs[0];

	}

	public Object getBucketId(MDIndexKey key) {
		return getBucketId(root, key);
	}

	public Object getBucketId(RNode node, MDIndexKey key) {
        if (value == null) {
            return start;
        }
        if (compareKey(value, dimension, type, key) > 0) {
            if (leftChild == null) {
                return start;
            }
            return leftChild.getBucketId(key, start*2);
        }
        else {
            if (rightChild == null) {
                return start;
            }
            return rightChild.getBucketId(key, start*2+1);
        }
	}

	public Bucket search(MDIndexKey key) {
		// TODO Auto-generated method stub
		return null;
	}

	public List<Bucket> range(MDIndexKey low, MDIndexKey high) {
		// TODO Auto-generated method stub
		return null;
	}

	public byte[] marshall() {
		// TODO Auto-generated method stub
		return null;
	}

	public void unmarshall(byte[] bytes) {
		// TODO Auto-generated method stub

	}
}
