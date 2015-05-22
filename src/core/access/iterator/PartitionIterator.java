package core.access.iterator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.lang.ArrayUtils;

import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;

import core.access.Partition;
import core.access.Predicate;
import core.utils.ReflectionUtils;

public class PartitionIterator implements Iterator<IteratorRecord> {

	public static enum ITERATOR {SCAN,FILTER,REPART};
	
	private IteratorRecord record;
	private byte[] recordBytes;

	private static char newLine = '\n';
	static char delimiter = '|';

	private byte[] bytes;
	private int bytesLength, offset, previous;

	protected Partition partition;
	
	protected Predicate[] predicates;

	public PartitionIterator(){
	}
	
	public PartitionIterator(String itrString){
	}
	
	
	public void setPartition(Partition partition){
		this.partition = partition;
		record = new IteratorRecord();
		bytes = partition.getBytes();
		//bytesLength = partition.getSize();
		bytesLength = bytes.length;
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
	    		if(isRelevant(record)){
	    			//System.out.println("relevant record found ..");
	    			return true;
	    		}
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
	
	public void finish() {
	}
	
	public void write(DataOutput out) throws IOException{
	}
	
	public void readFields(DataInput in) throws IOException{
	}
	
//	public static PartitionIterator read(DataInput in) throws IOException {
//		PartitionIterator it = new PartitionIterator();
//        it.readFields(in);
//        return it;
//	}
	
	
	public static String iteratorToString(PartitionIterator itr){
		ByteArrayDataOutput dat = ByteStreams.newDataOutput();
		try {
			itr.write(dat);
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("Failed to serialize the partitioner");
		}
		return itr.getClass().getName() +"@"+ new String(dat.toByteArray());		
	}
	
	public static PartitionIterator stringToIterator(String itrString){
		String[] tokens = itrString.split("@",2);
		return (PartitionIterator) ReflectionUtils.getInstance(tokens[0], new Class<?>[]{String.class}, new Object[]{tokens[1]});
	}
}
