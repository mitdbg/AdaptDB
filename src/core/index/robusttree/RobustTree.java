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
	double[] allocations;

	public static class Bound {
		public double upper;
		public double lower;

		public Bound(double upper, double lower) {
			this.upper = upper;
			this.lower = lower;
		}
	}

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
//		root.dimension = 0;
//		root.type = this.dimensionTypes[0];
//		root.value = this.histograms[0].quantile(0.5); // Get median
//
//		this.costs[0] = getAccessCost(0);

		int depth = 31 - Integer.numberOfLeadingZeros(this.maxBuckets); // Computes log(this.maxBuckets)
		double allocation = RobustTree.nthroot(this.numDimensions, this.maxBuckets);

		for (int i=0; i<this.numDimensions; i++) {
			this.allocations[i] = allocation;
		}

		this.createTree(root, depth, 2);
	}

	public void createTree(RNode node, int depth, double allocation) {
		if (depth > 0) {
			int dim = this.getLeastAllocated();
			node.dimension = dim;
			node.type = this.dimensionTypes[dim];

			Bound range = this.findRangeMidpoint(node.parent, node, dim);
			double rangeMidpoint = (range.upper + range.lower)/2;

			node.value = this.histograms[dim].quantile(rangeMidpoint); // Need to traverse up for range
			node.quantile = (float) 0.5;

			node.leftChild = new RNode();
			this.createTree(node.leftChild, depth - 1, allocation / 2);

			node.rightChild = new RNode();
			this.createTree(node.rightChild, depth - 1, allocation / 2);
		} else {
			Bucket b = new Bucket();
			node.bucket = b;
		}
	}

	/**
	 * Return the dimension which has the maximum
	 * allocation unfulfilled
	 * @return
	 */
	public int getLeastAllocated() {
		int index = 0;
		double alloc = this.allocations[0];
		for (int i=1; i<this.numDimensions; i++) {
			if (this.allocations[i] > alloc) {
				alloc = this.allocations[i];
				index = i;
			}
		}

		return index;
	}

	public Bound findRangeMidpoint(RNode node, RNode source, int dim) {
		// Happens only for the first node;
		if (node == null) {
			return new Bound(0, 1);
		} else if (node.parent != null) {
			Bound bound = this.findRangeMidpoint(node.parent, node, dim);

			if (node.dimension == dim) {
				if (node.leftChild == source) {
					bound.upper = node.quantile;
					return bound;
				} else {
					bound.lower = node.quantile;
					return bound;
				}
			} else {
				return bound;
			}
		} else {
			// root
			if (node.dimension == dim) {
				if (node.leftChild == source) {
					return new Bound(0, node.quantile);
				} else {
					return new Bound(node.quantile, 1);
				}
			} else {
				return new Bound(0, 1);
			}
		}
	}

	public Object getBucketId(MDIndexKey key) {
		return root.getBucketId(key);
	}

//	public Object getBucketId(RNode node, MDIndexKey key) {
//        if (value == null) {
//            return start;
//        }
//        if (compareKey(value, dimension, type, key) > 0) {
//            if (leftChild == null) {
//                return start;
//            }
//            return leftChild.getBucketId(key, start*2);
//        }
//        else {
//            if (rightChild == null) {
//                return start;
//            }
//            return rightChild.getBucketId(key, start*2+1);
//        }
//	}

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

	public static double nthroot(int n, double A) {
		return nthroot(n, A, .001);
	}

	public static double nthroot(int n, double A, double p) {
		if(A < 0) {
			System.err.println("A < 0");// we handle only real positive numbers
			return -1;
		} else if(A == 0) {
			return 0;
		}
		double x_prev = A;
		double x = A / n;  // starting "guessed" value...
		while(Math.abs(x - x_prev) > p) {
			x_prev = x;
			x = ((n - 1.0) * x + A / Math.pow(x, n - 1.0)) / n;
		}
		return x;
	}
}
