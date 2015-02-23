package core.adapt.partition.iterator;

import core.adapt.partition.Partition;
import core.index.key.CartilageIndexKey;
import core.utils.RangeUtils.Range;

public class ScanIterator extends PartitionIterator<CartilageIndexKey>{

	private Range range;
	
	private CartilageIndexKey key;
	char newLine = '\n';
	
	private byte[] bytes;
	private int offset, previous;
	
	
	public void setPartition(Partition partition, int attrIdx, Range range){
		this.partition = partition;
		this.range = range;
		
		key = new CartilageIndexKey('|', new int[]{attrIdx});
		bytes = partition.getBytes();
		offset = 0;
		previous = 0;
	}

	public boolean hasNext() {
		for ( ; offset<bytes.length; offset++ ){
	    	if(bytes[offset]==newLine){
	    		key.setBytes(bytes, previous, offset-previous);
	    		previous = ++offset;
	    		if(isLeft())
	    			continue;
	    		else
	    			return true;
	    	}
		}
		return false;
	}
	
	private boolean isLeft(){
		switch(key.types[0]){
		case BOOLEAN:	return range.isLeft(key.getBooleanAttribute(0));
		case INT:		return range.isLeft(key.getIntAttribute(0));
		case LONG:		return range.isLeft(key.getLongAttribute(0));
		case FLOAT:		return range.isLeft(key.getFloatAttribute(0));
		case DATE:		return range.isLeft(key.getDateAttribute(0));
		case STRING:	return range.isLeft(key.getStringAttribute(0,20));
		case VARCHAR:	return range.isLeft(key.getStringAttribute(0,100));
		default:		throw new RuntimeException("Invalid data type!");
		}
	}

	public CartilageIndexKey next() {
		return key;
	}
}
