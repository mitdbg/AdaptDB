package core.access.iterator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import core.access.Partition;
import core.access.Query.FilterQuery;
import core.index.robusttree.RNode;

public class RepartitionIterator extends PartitionIterator{

	private FilterQuery query;
	private RNode newIndexTree;

	protected Map<Integer,Partition> newPartitions;

	public RepartitionIterator(){
	}
	
	public RepartitionIterator(FilterQuery query, RNode newIndexTree){
		this.query = query;
		this.newIndexTree = newIndexTree;
	}

	@Override
	public void setPartition(Partition partition){
		super.setPartition(partition);
	}

	@Override
	protected boolean isRelevant(IteratorRecord record){

		int id = newIndexTree.getBucketId(record);
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

	@Override
	protected void finalize(){
		for(Partition p: newPartitions.values())
			p.store(true);
		partition.drop();
	}

	@Override
	public void write(DataOutput out) throws IOException{
		query.write(out);
		byte[] indexBytes = newIndexTree.marshall().getBytes();
//		out.writeBytes(newIndexTree.getClass().getName()+"\n");
		out.writeInt(indexBytes.length);
		out.write(indexBytes);
	}

	@Override
	public void readFields(DataInput in) throws IOException{
		query.readFields(in);
		newIndexTree = new RNode();
//		newIndexTree = (RNode)ReflectionUtils.getInstance(in.readLine());
		byte[] indexBytes = new byte[in.readInt()];
		in.readFully(indexBytes);
		newIndexTree.unmarshall(indexBytes);
	}
}
