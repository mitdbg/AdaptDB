package core.access.iterator;

import java.util.Iterator;

import org.apache.commons.lang.ArrayUtils;

import core.access.Partition;
import core.access.iterator.PartitionIterator.IteratorRecord;
import core.adapt.Predicate;
import core.index.key.CartilageIndexKey;

public class PartitionIterator implements Iterator<IteratorRecord>{

	public static enum ITER_TYPE {SCAN,SPLIT,CRACK};

	private IteratorRecord record;
	private byte[] recordBytes;

	private static char newLine = '\n';
	private static char delimiter = '|';

	private byte[] bytes;
	private int bytesLength, offset, previous;

	protected Partition partition;
	protected Predicate[] predicates;


	/**
	 * An wrapper class over CartilageIndexKey (to reuse much of the functionality)
	 *
	 * @author alekh
	 *
	 */
	public static class IteratorRecord extends CartilageIndexKey{

		public IteratorRecord() {
			super(PartitionIterator.delimiter);
		}

		public IteratorRecord(int[] keyAttrIdx){
			super(PartitionIterator.delimiter, keyAttrIdx);
		}

		public byte[] getBytes(){
			return this.bytes;
		}

		public int getOffset(){
			return this.offset;
		}

		public int getLength(){
			return this.length;
		}
	}

	public void setPartition(Partition partition, Predicate[] predicates){
		this.partition = partition;
		this.predicates = predicates;

		record = new IteratorRecord();
		bytes = partition.getBytes();
		bytesLength = partition.getSize();
		offset = 0;
		previous = 0;
	}

	public boolean hasNext() {
		for ( ; offset<bytesLength; offset++ ){
	    	if(bytes[offset]==newLine){
	    		//record.setBytes(bytes, previous, offset-previous);
	    		recordBytes = ArrayUtils.subarray(bytes, previous, offset);
	    		record.setBytes(recordBytes);
	    		previous = ++offset;
	    		if(isRelevant(record))
	    			return true;
	    		else
	    			continue;
	    	}
		}
		return false;
	}

	protected boolean isRelevant(IteratorRecord record){
		return true;
	}

	public void remove() {
		next();
	}

	public IteratorRecord next() {
		return record;
	}

}
