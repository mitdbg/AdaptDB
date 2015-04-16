package core.index.robusttree;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.udf.generic.NumericHistogram;

import core.adapt.Predicate;
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

		public Bound(double lower, double upper) {
			this.upper = upper;
			this.lower = lower;
		}
	}

	public RobustTree(int maxBuckets){
		this.initBuild(maxBuckets);
	}

	@Override
	public MDIndex clone() throws CloneNotSupportedException {
		// TODO Auto-generated method stub
		return null;
	}

	public void initBuild(int buckets) {
        this.maxBuckets = buckets;
		root = new RNode();
	}

	public RNode getRoot() {
		return root;
	}

	/***************************************************
	 ************* UPFRONT PARTITIONING ****************
	 ***************************************************/
	public void insert(MDIndexKey key) {
        if (dimensionTypes == null) {
            CartilageIndexKey k = (CartilageIndexKey)key;

        	this.dimensionTypes = k.detectTypes(true);
            this.numDimensions = dimensionTypes.length;

            this.histograms = new NumericHistogram[this.numDimensions];

            for (int i=0; i<numDimensions; i++) {
            	this.histograms[i] = new NumericHistogram();
            	this.histograms[i].allocate(10);
            }
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
		int depth = 31 - Integer.numberOfLeadingZeros(this.maxBuckets); // Computes log(this.maxBuckets)
		double allocation = RobustTree.nthroot(this.numDimensions, this.maxBuckets);

		this.allocations = new double[this.numDimensions];
		for (int i=0; i<this.numDimensions; i++) {
			this.allocations[i] = allocation;
		}

		this.createTree(root, depth, 2, 0);
	}

	public void createTree(RNode node, int depth, double allocation, long dimBitmap) {
		if (depth > 0) {
			int dim = this.getLeastAllocated(dimBitmap);
			node.attribute = dim;
			node.type = this.dimensionTypes[dim];
			this.allocations[dim] -= allocation;

			Bound range = this.findRangeMidpoint(node.parent, node, dim);
			double rangeMidpoint = (range.upper + range.lower)/2;

			node.value = this.histograms[dim].quantile(rangeMidpoint); // Need to traverse up for range
			node.quantile = (float) rangeMidpoint;

			dimBitmap = dimBitmap | 1 << dim;

			node.leftChild = new RNode();
			this.createTree(node.leftChild, depth - 1, allocation / 2, dimBitmap);

			node.rightChild = new RNode();
			this.createTree(node.rightChild, depth - 1, allocation / 2, dimBitmap);
		} else {
			Bucket b = new Bucket();
			node.bucket = b;
		}
	}

	static List<Integer> leastAllocated  = new ArrayList<Integer>();

	/**
	 * Return the dimension which has the maximum
	 * allocation unfulfilled
	 * @return
	 */
	public int getLeastAllocated(long dimBitmap) {
		leastAllocated.clear();
		leastAllocated.add(0);

		double alloc = this.allocations[0];
		for (int i=1; i<this.numDimensions; i++) {
			if (this.allocations[i] > alloc) {
				alloc = this.allocations[i];
				leastAllocated.clear();
				leastAllocated.add(i);
			} else if (this.allocations[i] == alloc) {
				leastAllocated.add(i);
			}
		}

		if (leastAllocated.size() == 1) {
			return leastAllocated.get(0);
		} else {
			for (int i=0; i < leastAllocated.size(); i++) {
				if ((dimBitmap & (1 << leastAllocated.get(i))) == 0) {
					return leastAllocated.get(i);
				}
			}
			return leastAllocated.get(0);
		}
	}

	public Bound findRangeMidpoint(RNode node, RNode source, int dim) {
		// Happens only for the first node;
		if (node == null) {
			return new Bound(0, 1);
		} else if (node.parent != null) {
			Bound bound = this.findRangeMidpoint(node.parent, node, dim);

			if (node.attribute == dim) {
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
			if (node.attribute == dim) {
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

	/**
	 * Used in the 2nd phase of upfront to assign each tuple to the right
	 */
	public Object getBucketId(MDIndexKey key) {
		return root.getBucketId(key);
	}

	/***************************************************
	 ***************** RUNTIME METHODS *****************
	 ***************************************************/

	public List<RNode> getMatchingBuckets(Predicate[] predicates) {
		List<RNode> results = root.search(predicates);
		return results;
	}

	public int getNumTuples(Predicate[] predicates) {
		int total = 0;
		List<RNode> matchingBuckets = getMatchingBuckets(predicates);
		// Note that the above list is a linked-list; don't use .get over it
		for (RNode node: matchingBuckets) {
			total += node.bucket.getNumTuples();
		}

		return total;
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

	/**
	 * Prints the tree created. Call only after initProbe is done.
	 */
	public void printTree() {
		printNode(root);
	}

	public static void printNode(RNode node) {
		if (node.bucket != null) {
			System.out.format("B");
		} else {
			System.out.format("Node: %d %f { ", node.attribute, node.quantile);
			printNode(node.leftChild);
			System.out.print(" }{ ");
			printNode(node.rightChild);
			System.out.print(" }");
		}
	}

	public List<Bucket> search(Predicate[] predicates) {
		// TODO Auto-generated method stub
		return null;
	}
}
