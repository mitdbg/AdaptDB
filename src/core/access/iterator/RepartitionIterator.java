package core.access.iterator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;

import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;

import core.access.Partition;
import core.access.Query.FilterQuery;
import core.index.MDIndex.BucketCounts;
import core.index.robusttree.RNode;

public class RepartitionIterator extends PartitionIterator{

	private FilterQuery query;
	private RNode newIndexTree;
	protected String zookeeperHosts;

	protected Map<Integer,Partition> newPartitions = new HashMap<Integer,Partition>();
	protected Map<Integer,Partition> oldPartitions = new HashMap<Integer,Partition>();

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
		oldPartitions.put(partition.getPartitionId(), partition);
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
		// p.write(record.getBytes(), record.getOffset(), record.getLength());
		p.write(record.getBytes(), 0, record.getBytes().length);

		return query.qualifies(record);
	}

	@Override
	public void finish(){
		if (zookeeperHosts != null) {
			BucketCounts c = new BucketCounts(zookeeperHosts);
			System.out.println("number of new partitions written = "+newPartitions.size());
			for(Partition p: newPartitions.values()){
				System.out.println("storing partition id "+p.getPartitionId());
				p.store(true);
				c.setToBucketCount(p.getPartitionId(), p.getRecordCount());
			}
			for(Partition p: oldPartitions.values()){
				System.out.println("dropping old partition id "+p.getPartitionId());
				p.drop();
				c.removeBucketCount(p.getPartitionId());
			}			
			c.close();
			oldPartitions = Maps.newHashMap();
			newPartitions = Maps.newHashMap();
			System.out.println("done finalize()");
		} else {
			System.out.println("INFO: Zookeeper Hosts NULL");
		}
	}

	@Override
	public void write(DataOutput out) throws IOException{
		query.write(out);
		byte[] indexBytes = newIndexTree.marshall().getBytes();
		out.writeInt(indexBytes.length);
		out.write(indexBytes);
		Text.writeString(out, zookeeperHosts);
		//out.writeBytes(zookeeperHosts+"\n");
	}

	@Override
	public void readFields(DataInput in) throws IOException{
		query = new FilterQuery();
		query.readFields(in);
		newIndexTree = new RNode();
		byte[] indexBytes = new byte[in.readInt()];
		in.readFully(indexBytes);
		newIndexTree.unmarshall(indexBytes);
		//zookeeperHosts = in.readLine();
		System.out.println("Initialized Tree");
		zookeeperHosts = Text.readString(in);
	}

	public static RepartitionIterator read(DataInput in) throws IOException {
		RepartitionIterator it = new RepartitionIterator();
        it.readFields(in);
        return it;
	}
}
