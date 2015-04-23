package core.access.iterator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import core.access.Partition;
import core.access.Query.FilterQuery;
import core.index.MDIndex;
import core.utils.ReflectionUtils;

public class RepartitionIterator extends PartitionIterator{

	private FilterQuery query;
	private MDIndex newIndexTree;
	
	private Map<Integer,Partition> newPartitions;
	
	public RepartitionIterator(FilterQuery query, MDIndex newIndexTree){
		this.query = query;
		this.newIndexTree = newIndexTree;
	}
	
	public void setPartition(Partition partition){
		super.setPartition(partition);
	}
	
	protected boolean isRelevant(IteratorRecord record){
		
		int id = (Integer)newIndexTree.getBucketId(record);		
		Partition p;
		if(newPartitions.containsKey(id)){
			p = newPartitions.get(id);
		}
		else{
			p = partition.clone();
			p.setPartitionId(id);
			newPartitions.put(id, p);
		}
		p.write(record.getBytes(), record.getOffset(), record.getLength());
		
		return query.qualifies(record);
	}
	
	protected void finalize(){
		for(Partition p: newPartitions.values())
			p.store(true);		
		partition.drop();
	}	
	
	public void write(DataOutput out) throws IOException{
		query.write(out);
		byte[] indexBytes = newIndexTree.marshall();
		out.writeBytes(newIndexTree.getClass().getName()+"\n");
		out.writeInt(indexBytes.length);
		out.write(indexBytes);
	}
	
	public void readFields(DataInput in) throws IOException{
		query.readFields(in);
		newIndexTree = (MDIndex)ReflectionUtils.getInstance(in.readLine());
		byte[] indexBytes = new byte[in.readInt()];
		in.readFully(indexBytes);
		newIndexTree.unmarshall(indexBytes);
	}
}
