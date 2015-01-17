package core.index.impl;

import java.util.List;

import core.index.MDIndex;

public class KDDTree implements MDIndex{

	@Override
	public void init(int dimensions, int buckets) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void insert(Object[] key) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void bulkLoad(Object[][] keys) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int getBucketId(Object[] key) {
		// TODO Auto-generated method stub
		return 0;
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

	@Override
	public Bucket search(Object[] key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Bucket> range(Object[] low, Object[] high) {
		// TODO Auto-generated method stub
		return null;
	}

}
