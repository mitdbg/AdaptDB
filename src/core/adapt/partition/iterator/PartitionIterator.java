package core.adapt.partition.iterator;

import java.util.Iterator;

import core.adapt.partition.Partition;
import core.utils.RangeUtils.Range;

public abstract class PartitionIterator<T> implements Iterator<T>{

	public static enum ITER_TYPE {SCAN,SPLIT,CRACK};
	
	protected Partition partition;

//	public PartitionIterator(Partition partition){
//		this.partition = partition;
//	}
	
	public void setPartition(Partition partition, int attrIdx, Range range){
	}
	
	public void remove() {
		next();		
	}
}
