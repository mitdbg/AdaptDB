package core.access.iterator;

import core.adapt.Predicate;
import core.utils.RangeUtils.Range;


public class PostFilterIterator extends PartitionIterator{

	@Override
	protected boolean isRelevant(IteratorRecord record){
		boolean qualify = true;
		for (Predicate p: predicates) {
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
