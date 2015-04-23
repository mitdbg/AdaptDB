package core.access.iterator;

import core.adapt.Predicate;


public class PostFilterIterator extends PartitionIterator{

	@Override
	protected boolean isRelevant(IteratorRecord record){
		boolean qualify = true;
		for (Predicate p: predicates) {
			int attrIdx = p.attribute;
			switch(record.types[attrIdx]){
			case BOOLEAN:	qualify &= p.isRelevant(record.getBooleanAttribute(attrIdx));
			case INT:		qualify &= p.isRelevant(record.getIntAttribute(attrIdx));
			case LONG:		qualify &= p.isRelevant(record.getLongAttribute(attrIdx));
			case FLOAT:		qualify &= p.isRelevant(record.getFloatAttribute(attrIdx));
			case DATE:		qualify &= p.isRelevant(record.getDateAttribute(attrIdx));
			case STRING:	qualify &= p.isRelevant(record.getStringAttribute(attrIdx,20));
			case VARCHAR:	qualify &= p.isRelevant(record.getStringAttribute(attrIdx,100));
			default:		throw new RuntimeException("Invalid data type!");
			}
		}
		return qualify;
	}

}
