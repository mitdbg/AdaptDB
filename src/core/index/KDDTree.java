package core.index;

import java.util.List;

import core.index.key.MDIndexKey;

public class KDDTree implements MDIndex{

	public KDDTree clone() throws CloneNotSupportedException{
		return null;
	}
	
	@Override
	public void initBuild(int buckets) {
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
	public Object getBucketId(MDIndexKey key) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Bucket search(MDIndexKey key) {
		// TODO Auto-generated method stub
		// very similar to the above method!
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
