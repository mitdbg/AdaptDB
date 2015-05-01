package core.index.robusttree;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import core.access.Predicate;
import core.index.MDIndex;
import core.index.key.CartilageIndexKey;
import core.index.key.CartilageIndexKeySet;
import core.index.key.MDIndexKey;
import core.utils.Pair;
import core.utils.SchemaUtils.TYPE;

public class RobustTreeHs implements MDIndex {
	public int maxBuckets;
	public int numAttributes;
    private CartilageIndexKeySet sample;
    private double samplingRate;

	public TYPE[] dimensionTypes;
	RNode root;

	public static class Bound {
		public double upper;
		public double lower;

		public Bound(double lower, double upper) {
			this.upper = upper;
			this.lower = lower;
		}
	}

	public RobustTreeHs(double samplingRate){
        this.samplingRate = samplingRate;
	}

	@Override
	public MDIndex clone() throws CloneNotSupportedException {
		return null;
	}

	@Override
	public void initBuild(int buckets) {
        this.maxBuckets = buckets;
		root = new RNode();
        this.sample = new CartilageIndexKeySet();
	}

	public RNode getRoot() {
		return root;
	}

	// Only used for testing
	public void setRoot(RNode root) {
		this.root = root;
	}

	@Override
	public boolean equals(Object obj) {
        if(obj instanceof RobustTreeHs){
        	RobustTreeHs rhs = (RobustTreeHs) obj;
        	boolean allGood = true;
        	allGood &= rhs.numAttributes == this.numAttributes;
        	allGood &= rhs.maxBuckets == this.maxBuckets;
        	allGood &= rhs.dimensionTypes.length == this.dimensionTypes.length;
        	if (!allGood)
        		return false;

        	for (int i=0; i<this.dimensionTypes.length; i++) {
        		allGood &= this.dimensionTypes[i] == rhs.dimensionTypes[i];
        	}
        	if (!allGood)
        		return false;

        	allGood = this.root == rhs.root;
        	return allGood;
        }
        return false;
    }

	/***************************************************
	 ************* UPFRONT PARTITIONING ****************
	 ***************************************************/
	@Override
	public void insert(MDIndexKey key) {
        CartilageIndexKey k = (CartilageIndexKey)key;

		if (dimensionTypes == null) {
        	this.dimensionTypes = k.detectTypes(true);
            this.numAttributes = dimensionTypes.length;
        }

        if (Math.random() < samplingRate) {
            this.sample.insert(k);
        }
	}

	@Override
	public void bulkLoad(MDIndexKey[] keys) {
		for (int i=0; i<keys.length; i++) {
			this.insert(keys[i]);
		}
	}


	/**
	 * Created the tree based on the histograms
	 */
	@Override
	public void initProbe() {
		int depth = 31 - Integer.numberOfLeadingZeros(this.maxBuckets); // Computes log(this.maxBuckets)
		double allocation = RobustTreeHs.nthroot(this.numAttributes, this.maxBuckets);

		double[] allocations = new double[this.numAttributes];
		for (int i=0; i<this.numAttributes; i++) {
			allocations[i] = allocation;
		}

		int[] counter = new int[this.numAttributes];

		this.createTree(root, depth, 2, counter, allocations, this.sample);
	}

	public void createTree(RNode node, int depth, double allocation, int[] counter, double[] allocations, CartilageIndexKeySet sample) {
		if (depth > 0) {
			int dim = getLeastAllocated(allocations, counter);
			node.attribute = dim;
			node.type = this.dimensionTypes[dim];
			allocations[dim] -= allocation;

			// TODO: This is brittle; Some places we might not have sample
			// OR there might no value - in this case we should choose some other dim
            Pair<CartilageIndexKeySet, CartilageIndexKeySet> halves = sample.sortAndSplit(dim);
			node.value = halves.first.getLast(dim);; // Need to traverse up for range

			counter[dim] += 1;

			node.leftChild = new RNode();
			this.createTree(node.leftChild, depth - 1, allocation / 2, counter, allocations, halves.first);

			node.rightChild = new RNode();
			this.createTree(node.rightChild, depth - 1, allocation / 2, counter, allocations, halves.second);

			counter[dim] -= 1;
		} else {
			Bucket b = new Bucket();
			b.setSample(sample);
			node.bucket = b;
		}
	}

	static List<Integer> leastAllocated  = new ArrayList<Integer>();

	/**
	 * Return the dimension which has the maximum
	 * allocation unfulfilled
	 * @return
	 */
	public static int getLeastAllocated(double[] allocations, int[] counter) {
		int numAttributes = allocations.length;
		assert allocations.length == counter.length;

		leastAllocated.clear();
		leastAllocated.add(0);

		double alloc = allocations[0];
		for (int i=1; i < numAttributes; i++) {
			if (allocations[i] > alloc) {
				alloc = allocations[i];
				leastAllocated.clear();
				leastAllocated.add(i);
			} else if (allocations[i] == alloc) {
				leastAllocated.add(i);
			}
		}

		if (leastAllocated.size() == 1) {
			return leastAllocated.get(0);
		} else {
			int count = counter[leastAllocated.get(0)];
			int index = 0;
			for (int i=1; i < leastAllocated.size(); i++) {
				int iCount = counter[leastAllocated.get(i)];
				if (iCount < count) {
					count = iCount;
					index = i;
				}
			}
			return index;
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
	@Override
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

	/**
	 * Serializes the index to string
	 * Very brittle - Consider rewriting
	 */
	@Override
	public byte[] marshall() {
		// JVM optimizes shit so no need to use string builder / buffer
		// Format:
		// maxBuckets, numAttributes
		// types
		// nodes in pre-order

		String robustTree = "";
		robustTree += String.format("%d %d\n", this.maxBuckets, this.numAttributes);

		String types = "";
		for (int i=0; i<this.numAttributes; i++) {
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

		this.dimensionTypes = new TYPE[this.numAttributes];
		for(int i=0; i<this.numAttributes; i++) {
			this.dimensionTypes[i] = TYPE.valueOf(sc.next());
		}

		this.root = new RNode();
		this.root.parseNode(sc);
	}

	public void loadSample(byte[] bytes) {
		this.sample.unmarshall(bytes);
		this.initializeBucketSamples(this.root, this.sample);
	}

	public void initializeBucketSamples(RNode n, CartilageIndexKeySet sample) {
		if (n.bucket != null) {
			n.bucket.setSample(sample);
		} else {
			// By sorting we avoid memory allocation
			// Will most probably be faster
			sample.sort(n.attribute);
			Pair<CartilageIndexKeySet, CartilageIndexKeySet> halves = sample.splitAt(n.attribute, n.value);
			initializeBucketSamples(n.leftChild, halves.first);
			initializeBucketSamples(n.rightChild, halves.second);
		}
	}

	public byte[] serializeSample() {
		return this.sample.marshall();
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
}
