package core.access.iterator;

import java.nio.ByteBuffer;

import org.apache.commons.lang.ArrayUtils;

import core.access.ReusablePartition;
import core.utils.BinaryUtils;

public class ReusablePartitionIterator extends PartitionIterator {

	protected ReusablePartition partition;
	
	public void setPartition(ReusablePartition partition){
		this.partition = partition;
		record = new IteratorRecord();
		ByteBuffer bb = partition.getNextBytes();
		bytes = bytes == null ? null : bb.array();
		bytesLength = bytes == null ? 0 : bb.limit();
		offset = bb.position();
		previous = bb.position();
		brokenRecordBytes = null;
	}

	public boolean hasNext() {
		for ( ; offset<bytesLength; offset++ ){
	    	if(bytes[offset]==newLine){
	    		//record.setBytes(bytes, previous, offset-previous);
	    		recordBytes = ArrayUtils.subarray(bytes, previous, offset);
				if(brokenRecordBytes!=null) {
					recordBytes = BinaryUtils.concatenate(brokenRecordBytes, recordBytes);
					brokenRecordBytes = null;
				}
				try {
					record.setBytes(recordBytes);
				} catch (ArrayIndexOutOfBoundsException e) {
					System.out.println("Index out of bounds while setting bytes: "+(new String(recordBytes)));
					throw e;
				}
	    		previous = ++offset;
	    		if(isRelevant(record)){
	    			//System.out.println("relevant record found ..");
	    			return true;
	    		}
	    		else
	    			continue;
	    	}
		}
		
		if(previous < bytesLength)
			brokenRecordBytes = BinaryUtils.getBytes(bytes, previous, bytesLength-previous);
		else
			brokenRecordBytes = null;

		ByteBuffer bb = partition.getNextBytes();
		bytes = bytes == null ? null : bb.array();
		//bytes = partition == null ? null : partition.getNextBytes();
		if(bytes!=null){
			bytesLength = bb.limit();
			offset = bb.position();
			previous = bb.position();
			return hasNext();
		}
		else
			return false;
	}
}
