package core.access.iterator;

import core.access.Partition;
import core.adapt.Predicate;
import core.adapt.partition.merger.PartitionMerger;

public class RepartitionIterator extends PartitionIterator{

	private PartitionMerger merger;
	private Partition nonRelevantP, relevantP;

	public RepartitionIterator(PartitionMerger merger){
		this.merger = merger;
	}

	@Override
	public void setPartition(Partition partition, Predicate[] predicates){
		super.setPartition(partition, predicates);

		if(nonRelevantP==null)
			nonRelevantP = partition.createChild(0);
		else
			nonRelevantP.setAsParent(partition);


		if(relevantP==null)
			relevantP = partition.createChild(1);
		else
			relevantP.setAsParent(partition);
	}

	@Override
	protected boolean isRelevant(IteratorRecord record){
		if(super.isRelevant(record)){
			relevantP.write(record.getBytes(), record.getOffset(), record.getLength());			// store in the right partition
			return true;
		}
		else{
			nonRelevantP.write(record.getBytes(), record.getOffset(), record.getLength());		// store in the left partition
			return false;
		}
	}

	@Override
	protected void finalize(){
		Partition[] mergedPartitions = merger.getMergedPartitions(relevantP, nonRelevantP);
		for(Partition p: mergedPartitions)
			p.store(true);
		//TODO: need to adjust the tree as well ...

//		merger.getMergedPartition(nonRelevantP).store(true);
//		merger.getMergedPartition(relevantP).store(true);

		partition.drop();
	}

	public Partition getLeft(){
		return nonRelevantP;
	}

	public Partition getRight(){
		return relevantP;
	}
}
