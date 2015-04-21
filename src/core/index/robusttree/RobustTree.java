package core.index.robusttree;

import java.text.Format;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.hive.ql.udf.generic.NumericHistogram;

import core.access.Predicate;
import core.index.MDIndex;
import core.index.key.CartilageIndexKey;
import core.index.key.MDIndexKey;
import core.utils.SchemaUtils.TYPE;

public class RobustTree implements MDIndex {
	int maxBuckets;
	int numAttributes;
	NumericHistogram[] histograms;
	TYPE[] dimensionTypes;
	RNode root;

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
            this.numAttributes = dimensionTypes.length;

            this.histograms = new NumericHistogram[this.numAttributes];

            for (int i=0; i<numAttributes; i++) {
            	this.histograms[i] = new NumericHistogram();
            	this.histograms[i].allocate(10);
            }
        }

        for (int i=0; i<numAttributes; i++) {
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
		double allocation = RobustTree.nthroot(this.numAttributes, this.maxBuckets);

		double[] allocations = new double[this.numAttributes];
		for (int i=0; i<this.numAttributes; i++) {
			allocations[i] = allocation;
		}

		int[] counter = new int[this.numAttributes];

		this.createTree(root, depth, 2, counter, allocations);
	}

	public void createTree(RNode node, int depth, double allocation, int[] counter, double[] allocations) {
		if (depth > 0) {
			int dim = getLeastAllocated(allocations, counter);
			node.attribute = dim;
			node.type = this.dimensionTypes[dim];
			allocations[dim] -= allocation;

			Bound range = this.findRangeMidpoint(node.parent, node, dim);
			double rangeMidpoint = (range.upper + range.lower)/2;

			node.value = this.histograms[dim].quantile(rangeMidpoint); // Need to traverse up for range
			node.quantile = (float) rangeMidpoint;

			counter[dim] += 1;

			node.leftChild = new RNode();
			this.createTree(node.leftChild, depth - 1, allocation / 2, counter, allocations);

			node.rightChild = new RNode();
			this.createTree(node.rightChild, depth - 1, allocation / 2, counter, allocations);

			counter[dim] -= 1;
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

	/**
	 * Serializes the index to string
	 * Very brittle - Consider rewriting
	 */
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

		LinkedList<RNode> stack = new LinkedList<RNode>();
		stack.add(root);
		while (stack.size() != 0) {
			RNode n = stack.removeLast();
			String nStr;
			if (n.bucket == null) {
				nStr = String.format("b %d %d \n", n.bucket.getBucketId(), n.bucket.getNumTuples());
			} else {
				if (n.type == TYPE.DATE) {
					Format formatter = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
					nStr = String.format("n %d %d %s\n", n.attribute, n.type.toString(), formatter.format(n.value));
				} else {
					nStr = String.format("n %d %d %s\n", n.attribute, n.type.toString(), n.value.toString());
				}

				stack.add(n.rightChild);
				stack.add(n.leftChild);
			}
			robustTree += nStr;
		}

		return robustTree.getBytes();
	}

	public void unmarshall(byte[] bytes) {
		String tree = bytes.toString();
		Scanner sc = new Scanner(tree);
		this.maxBuckets = sc.nextInt();
		this.numAttributes = sc.nextInt();

		this.dimensionTypes = new TYPE[this.numAttributes];
		for(int i=0; i<this.numAttributes; i++) {
			this.dimensionTypes[i] = TYPE.valueOf(sc.next());
		}

		this.root = this.parseNode(sc);
	}

	public RNode parseNode(Scanner sc) {
		String type = sc.next();
		RNode r = new RNode();
		if (type == "n") {
			r.attribute = sc.nextInt();
			r.type = TYPE.valueOf(sc.next());

			switch (r.type) {
			case INT:
				r.value = sc.nextInt();
				break;
			case FLOAT:
				r.value = sc.nextFloat();
				break;
			case LONG:
				r.value = sc.nextLong();
				break;
			case DATE:
				Format formatter = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
				try {
					r.value = formatter.parseObject(sc.next());
				} catch (ParseException e) {
					e.printStackTrace();
				}
				break;
			case BOOLEAN:
				r.value = sc.nextBoolean();
				break;
			case STRING:
				r.value = sc.next();
				break;
			case VARCHAR:
				r.value = sc.next();
				break;
			}

			r.leftChild = this.parseNode(sc);
			r.rightChild = this.parseNode(sc);
		} else if (type == "b") {
			Bucket b = new Bucket();
			b.setBucketId(sc.nextInt());
			b.setNumTuples(sc.nextInt());
			r.bucket = b;
		} else {
			System.out.println("Bad things have happened in unmarshall");
		}

		return r;
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
