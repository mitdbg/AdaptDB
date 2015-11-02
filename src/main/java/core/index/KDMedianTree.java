package core.index;

import core.key.RawIndexKey;
import core.key.ParsedTupleList;
import core.utils.Pair;
import core.utils.TypeUtils.TYPE;

import java.util.*;

/**
 * Created by qui on 2/19/15.
 */
public class KDMedianTree implements MDIndex {

	private List<ParsedTupleList> buckets;
	private double samplingRate;
	TYPE[] dimensionTypes;
	int[] attrOrder;
	Map<Integer, Integer> nextAttrIdx = new HashMap<Integer, Integer>();
	int maxBuckets;
	KDNode root;

	public KDMedianTree(double samplingRate) {
		this(samplingRate, null);
	}

	public KDMedianTree(double samplingRate, int[] attrs) {
		this.attrOrder = attrs;
		this.samplingRate = samplingRate;
	}

	@Override
	public MDIndex clone() {
		return new KDMedianTree(samplingRate, attrOrder);
	}

	@Override
	public void initBuild(int numBuckets) {
		int numLevels = (int) Math.ceil(Math.log(numBuckets) / Math.log(2));
		int numBucketsEven = (int) Math.pow(2, numLevels);
		this.maxBuckets = numBucketsEven;
		this.root = new KDNode();
		buckets = new ArrayList<ParsedTupleList>();
		buckets.add(new ParsedTupleList());
	}

	public void insert(RawIndexKey key) {
		RawIndexKey k = (RawIndexKey) key;
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
			if (i < numSplits - 1) { // last level doesn't count towards
										// allocation
				allocations[dimToSplit] += numNodesAdded / Math.pow(2, i - 1);
			}
			currentDim = dimToSplit;
		}
		System.out.println("Allocations: " + Arrays.toString(allocations));
	}

	private int splitTree(List<ParsedTupleList> buckets, int dim) {
		int n = buckets.size();
		int numNodesAdded = 0;
		for (int i = 0; i < n; i++) {
			ParsedTupleList keys = buckets.remove(0);
			if (keys.size() > 1) {
				Pair<ParsedTupleList, ParsedTupleList> halves = keys
						.sortAndSplit(dim);
				buckets.add(halves.first);
				buckets.add(halves.second);
				RawIndexKey key = halves.second.iterator().next();
				this.insert(key);
				numNodesAdded++;
			}
		}
		return numNodesAdded;
	}

	void initializeDimensions(RawIndexKey key) {
		dimensionTypes = key.getTypes(true);
		int numDimensions = dimensionTypes.length;
		int[] keys = key.getKeys();

		if (attrOrder == null) {
			nextAttrIdx.put(-1, 0);
			for (int i = 0; i < keys.length; i++) {
				nextAttrIdx.put(i, (i + 1) % numDimensions);
			}
		} else {
			Map<Integer, Integer> keyToIndex = new HashMap<Integer, Integer>();
			for (int i = 0; i < keys.length; i++) {
				keyToIndex.put(keys[i], i);
			}
			nextAttrIdx.put(-1, keyToIndex.get(attrOrder[0]));
			for (int i = 0; i < attrOrder.length; i++) {
				nextAttrIdx.put(keyToIndex.get(attrOrder[i]),
						keyToIndex.get(attrOrder[(i + 1) % numDimensions]));
			}
		}
	}
	
	public Object getBucketId(RawIndexKey key) {
		return Integer.toString(this.root.getBucketId(key, 1));
	}

	public byte[] marshall() {
		// TODO Auto-generated method stub
		return null;
	}

	public void unmarshall(byte[] bytes) {
		// TODO Auto-generated method stub

	}
}
