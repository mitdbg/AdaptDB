package core.common.index;

import java.util.HashMap;

import core.common.key.ParsedTupleList;
import core.common.key.RawIndexKey;
import core.utils.Range;
import core.utils.TypeUtils;

/**
 *
 * An API for implementing multi-dimensional index, e.g. R-Tree, K-d Tree, etc.
 *
 */
public interface MDIndex {

	/*
	 * Placeholder class for the index leaves.
	 */
	final class Bucket {
		/* Actual Values */
		int bucketId;
		ParsedTupleList sample;

		/* Estimates */
		private double estimatedTuples = 0;

		public static int maxBucketId = 0;

		private static HashMap<Integer, Double> counter = new HashMap<Integer, Double>();

		public Bucket() {
			bucketId = maxBucketId;
			maxBucketId += 1;
		}

		public Bucket(int id) {
			bucketId = id;
			maxBucketId = Math.max(bucketId + 1, maxBucketId);
		}

		public double getEstimatedNumTuples() {
			// TODO: This call used when restoring a replaced tree in Optimizer.
			// Can't use the assert below.
			// Assert.assertNotEquals(estimatedTuples, 0.0);
			return estimatedTuples;
		}

		public static double getEstimatedNumTuples(int bucketId) {
			Double val = counter.get(bucketId);
			return val;
		}

		public void setEstimatedNumTuples(double num) {
			estimatedTuples = num;
			counter.put(bucketId, num);
		}

		public int getBucketId() {
			return bucketId;
		}

		public void updateId() {
			this.bucketId = maxBucketId;
			maxBucketId += 1;
			estimatedTuples = 0;
		}

		public ParsedTupleList getSample() {
			return sample;
		}

		public void setSample(ParsedTupleList sample) {
			this.sample = sample;
		}
	}

//	public static class BucketCounts {
//
//		private CuratorFramework client;
//		private String counterPathBase = "/partition-count-";
//
//		public BucketCounts(String zookeeperHosts) {
//			client = CuratorUtils.createAndStartClient(zookeeperHosts);
//		}
//
//		public BucketCounts(CuratorFramework client) {
//			this.client = client;
//		}
//
//		public void addToBucketCount(int bucketId, int count) {
//			CuratorUtils.addCounter(client, counterPathBase + bucketId, count);
//		}
//
//		public int getBucketCount(int bucketId) {
//			return CuratorUtils.getCounter(client, counterPathBase + bucketId);
//		}
//
//		public void setToBucketCount(int bucketId, int count) {
//			CuratorUtils.setCounter(client, counterPathBase + bucketId, count);
//		}
//
//		public void removeBucketCount(int bucketId) {
//			CuratorUtils.setCounter(client, counterPathBase + bucketId, 0);
//		}
//
//		public void close() {
//			client.close();
//		}
//
//		public CuratorFramework getClient() {
//			return this.client;
//		}
//	}

	public MDIndex clone() throws CloneNotSupportedException;

	/*
	 *
	 * The Build phase of the index
	 */

	void setMaxBuckets(int maxBuckets);

	int getMaxBuckets();

	/*
	 *
	 * The Probe phase of the index
	 */
	public void initProbe();

	public void initProbe(int joinAttribute);

	/**
	 * Get the bucket id, for a given key, from an existing index.
	 *
	 * @param key
	 * @return
	 */
	public Object getBucketId(RawIndexKey key);

	/**
	 * Serialize the index into a byte array.
	 *
	 * @return serialized index.
	 */
	public byte[] marshall();

	/**
	 * Deserialize the index from a byte array.
	 *
	 * @param bytes
	 */
	public void unmarshall(byte[] bytes);

	/**
	 * Created by qui on 7/9/15.
	 */
	class BucketInfo extends Range {

		private int id;

		public BucketInfo(int id, TypeUtils.TYPE type, Object low, Object high) {
			super(type, low, high);
			this.id = id;
		}

		public BucketInfo(TypeUtils.TYPE type, Object low, Object high) {
			this(0, type, low, high);
		}

		public void setId(int id) {
			this.id = id;
		}

		public int getId() {
			return id;
		}

		@Override
		public BucketInfo clone() {
			return new BucketInfo(id, getType(), getLow(), getHigh());
		}
	}
}
