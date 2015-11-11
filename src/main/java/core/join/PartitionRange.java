package core.join;

import core.common.index.MDIndex;
import core.utils.Range;
import core.utils.TypeUtils.TYPE;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by qui on 7/9/15.
 */
public class PartitionRange extends Range {

	private Map<Integer, MDIndex.BucketInfo> buckets;

	public PartitionRange(TYPE type, Object low, Object high) {
		super(type, low, high);
		this.buckets = new HashMap<Integer, MDIndex.BucketInfo>();
	}

	public void addBucket(MDIndex.BucketInfo bucketInfo) {
		addBucket(bucketInfo, true);
	}

	public void addBucket(MDIndex.BucketInfo bucketInfo, boolean updateRange) {
		if (buckets.containsKey(bucketInfo.getId())) {
			buckets.get(bucketInfo.getId()).union(bucketInfo);
		} else {
			buckets.put(bucketInfo.getId(), bucketInfo);
		}
		if (updateRange) {
			this.union(bucketInfo);
		}
	}

	public List<MDIndex.BucketInfo> getBuckets() {
		return new ArrayList<MDIndex.BucketInfo>(buckets.values());
	}

	public int getNumBuckets() {
		return buckets.size();
	}

	@Override
	public String toString() {
		return super.toString() + ", " + getNumBuckets();
	}
}
