package core.index;
import java.util.List;


/**
 * 
 * An API for implementing multi-dimensional index, e.g. R-Tree, K-d Tree, etc.
 * 
 */

public interface MDIndex {

	/**
	 * Placeholder class for the index leaves.
	 * 
	 */
	public static class Bucket{
		int bucketId;
	}
	
	
	/**
	 * Initialize the index with the number of dimensions and 
	 * the maximum number of buckets over those dimensions.
	 * 
	 * @param dimensions
	 * @param buckets
	 */
	public void init(int dimensions, int buckets);

	
	/**
	 * Insert an entry into the index structure (internal nodes).
	 * This method does not load the actual data into the index.
	 * 
	 * @param key
	 */
	public void insert(Object[] key);
	

	/**
	 * Bulk load the index structure, without loading the actual data.
	 * 
	 * @param keys
	 */
	public void bulkLoad(Object[][] keys);
	
	
	/**
	 * Get the bucket id, for a given key, from an existing index.
	 * 
	 * @param key
	 * @return
	 */
	public int getBucketId(Object[] key);
	
	
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
	 * Point query.
	 * 
	 * @param key
	 * @return the bucket containing the key.
	 */
	public Bucket search(Object[] key);
	
	
	/**
	 * Range query.
	 * 
	 * @param low
	 * @param high
	 * @return the set of buckets containing the given range.
	 */
	public List<Bucket> range(Object[] low, Object[] high);
}
