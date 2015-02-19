package core.adapt.split;

import java.util.List;

import core.adapt.partition.DirtyPartitionBuffer;
import core.adapt.partition.Partition;
import core.adapt.partition.iterator.PartitionIterator;
import core.index.key.CartilageIndexKey2;


/**
 * This class iterates over all tuples in all partitions of a split.
 * 
 * @author alekh
 *
 */
public abstract class SplitIterator {

	
	protected List<Partition> splitPartitions;
	protected PartitionIterator<CartilageIndexKey2> currPartitionItr;
	protected int currentPartitionIdx;
	
	public SplitIterator(List<Partition> splitPartitions){
		this.splitPartitions = splitPartitions;
	}
	
	public boolean hasNext() {
		if(currPartitionItr!=null && currPartitionItr.hasNext()){
			return true;
		}
		else if(currentPartitionIdx < splitPartitions.size()){
			// TODO: check if we need to merge/flush the buffer before loading next partition  
			currPartitionItr = getPartitionIterator();
			currentPartitionIdx++;
			return true;
		}
		else{
			finalize();
			return false;
		}
	}
	
	protected abstract PartitionIterator<CartilageIndexKey2> getPartitionIterator();
	
	public CartilageIndexKey2 next() {
		return currPartitionItr.next();
	}

	public void remove() {
		next();
	}
	
	protected void finalize(){
	}
	
	
	
	public static class ScanSplitIterator extends SplitIterator{
		public ScanSplitIterator(List<Partition> splitPartitions) {
			super(splitPartitions);
		}
		protected PartitionIterator getPartitionIterator(){
			splitPartitions.get(currentPartitionIdx).load();
			// TODO: return scan iterator
			return null;
		}
	}
	
	public static class CrackSplitIterator extends SplitIterator{
		protected DirtyPartitionBuffer buffer;	// buffer to keep track of modified partitions
		
		public CrackSplitIterator(List<Partition> splitPartitions) {
			super(splitPartitions);
			buffer = new DirtyPartitionBuffer();
		}
		protected PartitionIterator getPartitionIterator(){
			Partition p = splitPartitions.get(currentPartitionIdx);
			buffer.add(p);			
			p.load();
			// TODO: return crack iterator
			return null;
		}
		protected void finalize(){
			// TODO: buffer.mergeAll(unpartitionAttribute);
			buffer.evictAll();
		}
	}
}
