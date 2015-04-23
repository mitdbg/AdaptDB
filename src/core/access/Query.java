package core.access;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.google.common.base.Joiner;

import core.access.iterator.PartitionIterator.IteratorRecord;
import core.utils.RangeUtils.Range;


public class Query {
	protected Predicate[] predicates;
	
	public Predicate[] getPredicates() {
		return this.predicates;
	}
	
	public void write(DataOutput out) throws IOException{
		out.writeBytes(Joiner.on(",").join(predicates)+"\n");
	}
	
	public void readFields(DataInput in) throws IOException{
		String[] tokens = in.readLine().split(",");
		predicates = new Predicate[tokens.length];
		for(int i=0; i<predicates.length; i++)
			predicates[i] = new Predicate(tokens[i]);		
	}
	
	
	public static class FilterQuery extends Query{
	
		public FilterQuery() {
			this.predicates = null;
		}

		public FilterQuery(Predicate[] predicates) {
			this.predicates = predicates;
		}
		
		public boolean qualifies(IteratorRecord record){
			boolean qualify = true;
			for(Predicate p: predicates){
				Range range = p.getRange();
				int attrIdx = p.getAttribute();
				switch(record.types[attrIdx]){
				case BOOLEAN:	qualify &= range.contains(record.getBooleanAttribute(attrIdx));
				case INT:		qualify &= range.contains(record.getIntAttribute(attrIdx));
				case LONG:		qualify &= range.contains(record.getLongAttribute(attrIdx));
				case FLOAT:		qualify &= range.contains(record.getFloatAttribute(attrIdx));
				case DATE:		qualify &= range.contains(record.getDateAttribute(attrIdx));
				case STRING:	qualify &= range.contains(record.getStringAttribute(attrIdx,20));
				case VARCHAR:	qualify &= range.contains(record.getStringAttribute(attrIdx,100));
				default:		throw new RuntimeException("Invalid data type!");
				}
			}
			return qualify;
		}
	}

	public class JoinQuery extends Query{
		//TODO:
	}
}
