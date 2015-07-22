package core.access.spark.join.algo;

public class HyperJoinOverlapReplicaTuned {

	/*
	 * Things to consider:
	 * 
	 * 1. the available memory
	 * 2. number of splits
	 * 3. size of hash table --> 
	 * 4. the total input sizes
	 *  
	 *  
	 * max split size 
	 */

	
	/*
	 * What options do we have:
	 * 
	 *  1. extend existing split [other table will scan more]
	 *  2. create a new split, when we reach the ahs table size limit 
	 *  3. cut the bucket into two --> try 1. and 2. again
	 *  
	 *  
	 *  
	 *  
	 *  Two STep algo:
	 *  
	 *  1. cutting -- cut buckets that span too much into multiple (virtual) ones so that the hash table fits in memory [here, each bucket is potentially a split]
	 *  2. combine -- so that we reduce the total input size, while keeping the hash table size within limit, and stopping as soon as we reach the minimum number of splits 
	 * 
	 */
}
