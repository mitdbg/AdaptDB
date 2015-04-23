package core.access.iterator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import core.access.Query.FilterQuery;

public class PostFilterIterator extends PartitionIterator{

	protected FilterQuery query;

	public PostFilterIterator(){
	}

	public PostFilterIterator(FilterQuery query){
		this.query = query;
	}

	@Override
	protected boolean isRelevant(IteratorRecord record){
		return query.qualifies(record);
	}

	@Override
	public void write(DataOutput out) throws IOException{
		query.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException{
		query.readFields(in);
	}
}
