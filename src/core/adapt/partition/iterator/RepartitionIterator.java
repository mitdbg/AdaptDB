package core.adapt.partition.iterator;

import core.adapt.partition.Partition;
import core.adapt.partition.merger.PartitionMerger;
import core.index.key.CartilageIndexKey2;
import core.utils.RangeUtils.Range;

public class RepartitionIterator extends PartitionIterator<CartilageIndexKey2>{

	private Range range;
	
	private CartilageIndexKey2 key;
	char newLine = '\n';
	
	private byte[] bytes;
	private int offset, previous;
	
	private PartitionMerger merger;
	private Partition leftP, rightP;
	

	public RepartitionIterator(PartitionMerger merger){
		this.merger = merger;
	}
	
	public void setPartition(Partition partition, int attrIdx, Range range){
		this.partition = partition;
		this.range = range;
		
		key = new CartilageIndexKey2('|', new int[]{attrIdx});
		bytes = partition.getBytes();
		offset = 0;
		previous = 0;
		
		if(leftP==null)
			leftP = partition.createChild(0);
		else
			leftP.setAsParent(partition);
		
		
		if(rightP==null)
			rightP = partition.createChild(1);
		else
			rightP.setAsParent(partition);
	}
	
	public boolean hasNext() {
		for ( ; offset<bytes.length; offset++ ){
	    	if(bytes[offset]==newLine){
	    		key.setBytes(bytes, previous, offset-previous);
	    		if(isLeft()){
	    			leftP.write(bytes, previous, offset-previous);	// store in the left partition
	    			previous = ++offset;
	    			continue;
	    		}
	    		else{
	    			rightP.write(bytes, previous, offset-previous);	// store in the right partition
	    			previous = ++offset;
	    			return true;
	    		}
	    	}
		}
		finalize();
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

	public CartilageIndexKey2 next() {
		return key;
	}
	
	protected void finalize(){
		merger.getMergedPartition(leftP).store(true);
		merger.getMergedPartition(rightP).store(true);
		partition.drop();
	}
	
	public Partition getLeft(){
		return leftP;
	}
	
	public Partition getRight(){
		return rightP;
	}
}
