package core.access.iterator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import com.google.common.io.ByteStreams;

import core.access.Partition;
import core.access.Query.FilterQuery;
import core.index.MDIndex.BucketCounts;
import core.index.robusttree.RNode;

public class RepartitionIterator extends PartitionIterator{

	private FilterQuery query;
	private RNode newIndexTree;
	protected String zookeeperHosts;

	protected Map<Integer,Partition> newPartitions;

	public RepartitionIterator(){
	}
	
	public RepartitionIterator(String iteratorString){
		try {
			readFields(ByteStreams.newDataInput(iteratorString.getBytes()));
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("Failed to read the fields");
		}
	}

	public RepartitionIterator(FilterQuery query, RNode newIndexTree){
		this.query = query;
		this.newIndexTree = newIndexTree;
	}
	
	public void setZookeeper(String zookeeperHosts){
		this.zookeeperHosts = zookeeperHosts;
	}
	
	public DistributedRepartitionIterator createDistributedIterator(){
		DistributedRepartitionIterator itr = new DistributedRepartitionIterator(query, newIndexTree);
		itr.setZookeeper(zookeeperHosts);
		return itr;
	}

	public FilterQuery getQuery(){
		return this.query;
	}
	
	public RNode getIndexTree(){
		return this.newIndexTree;
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
		BucketCounts c = new BucketCounts(zookeeperHosts);
		for(Partition p: newPartitions.values()){
			p.store(true);
			c.setToBucketCount(p.getPartitionId(), p.getRecordCount());			
		}
		partition.drop();
		c.removeBucketCount(partition.getPartitionId());
		c.close();
	}

	@Override
	public void write(DataOutput out) throws IOException{
		query.write(out);
		byte[] indexBytes = newIndexTree.marshall().getBytes();
		out.writeInt(indexBytes.length);
		out.write(indexBytes);
		out.writeBytes(zookeeperHosts+"\n");
	}

	@Override
	public void readFields(DataInput in) throws IOException{
		query = new FilterQuery();
		query.readFields(in);
		newIndexTree = new RNode();
		byte[] indexBytes = new byte[in.readInt()];
		in.readFully(indexBytes);
		newIndexTree.unmarshall(indexBytes);
		zookeeperHosts = in.readLine();
	}
	
	public static RepartitionIterator read(DataInput in) throws IOException {
		RepartitionIterator it = new RepartitionIterator();
        it.readFields(in);
        return it;
	}	
}
