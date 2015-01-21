package core.index;

import java.util.List;

import core.index.key.MDIndexKey;
import core.utils.SchemaUtils.TYPE;

/**
 * A simple range tree which collects the ranges of all index 
 * attributes in the build phase and constructs a range tree over that. 
 * 
 * @author alekh
 *
 */
public class SimpleRangeTree implements MDIndex {

	@Override
	public void initBuild(TYPE[] dimensions, int buckets) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void insert(MDIndexKey key) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void bulkLoad(MDIndexKey[] keys) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void initProbe() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int getBucketId(MDIndexKey key) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Bucket search(MDIndexKey key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Bucket> range(MDIndexKey low, MDIndexKey high) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] marshall() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void unmarshall(byte[] bytes) {
		// TODO Auto-generated method stub
		
	}

}
