package core.index.key;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Lists;

import core.utils.Pair;
import core.utils.RangeUtils.SimpleDateRange.SimpleDate;
import core.utils.SchemaUtils.TYPE;

/**
 * A class to collect a set of keys during index building.
 * Example use case is to collect samples before inserting them into an index structure. 
 * 
 * @author alekh
 *
 */
public class CartilageIndexKeySet {

	private List<Object[]> values;
	private TYPE[] types; 
	
	
	/**
	 * Create an empty key set.
	 */
	public CartilageIndexKeySet() {
		values = Lists.newArrayList();
	}
	
	/**
	 * Instantiate a key set with a given set of keys.
	 * @param values
	 */
	public CartilageIndexKeySet(List<Object[]> values) {
		this.values = values;
	}
	
	/**
	 * Insert into the key set.
	 * @param key
	 */
	public void insert(CartilageIndexKey key){
		if(types==null)
			this.types = key.types;
		
		Object[] keyValues = new Object[types.length];
		for(int i=0; i<types.length; i++){
			switch(types[i]){
			case INT:		keyValues[i] = key.getIntAttribute(i);
							break;
			case LONG:		keyValues[i] = key.getLongAttribute(i);
							break;				
			case FLOAT:		keyValues[i] = key.getFloatAttribute(i);
							break;				
			case DATE:		keyValues[i] = key.getDateAttribute(i);
							break;				
			case STRING:	keyValues[i] = key.getStringAttribute(i,20); 
							break;
			case VARCHAR:	break; // skip partitioning on varchar attribute
			default:		throw new RuntimeException("Unknown dimension type: "+key.types[i]);
			}
		}
		values.add(keyValues);
	}
	
	/**
	 * Sort the key set on the given attribute.
	 *
	 * @param attributeIdx -- the index of the sort attribute relative to the key attributes.
	 * 
	 * Example -- if we have 5 attributes ids (0,1,2,3,4) and we extract 3 of them
	 * in the CartilageIndexKey, e.g. (1,3,4) and now if we want to sort on attribute
	 * 3 then the attributeIdx parameter will pass 1 (sorting on the second key attribute).
	 * 
	 */
	public void sort(final int attributeIdx){
		final TYPE sortType = types[attributeIdx]; 
		Collections.sort(values, new Comparator<Object[]> (){
			public int compare(Object[] o1, Object[] o2) {
				switch(sortType){
				case INT:		return ((Integer)o1[attributeIdx]).compareTo((Integer)o2[attributeIdx]);
				case LONG:		return ((Long)o1[attributeIdx]).compareTo((Long)o2[attributeIdx]);				
				case FLOAT:		return ((Float)o1[attributeIdx]).compareTo((Float)o2[attributeIdx]);				
				case DATE:		return ((SimpleDate)o1[attributeIdx]).compareTo((SimpleDate)o2[attributeIdx]);				
				case STRING:	return ((String)o1[attributeIdx]).compareTo((String)o2[attributeIdx]);
				case VARCHAR:	throw new RuntimeException("sorting over varchar is not supported"); // skip partitioning on varchar attribute
				default:		throw new RuntimeException("Unknown dimension type: "+sortType);
				}
			}
		});
	}
	
	/**
	 * Split this key set into two equal sized key sets.
	 * This function assumes that the key set has already been sorted.
	 *
	 * Note that we do not copy the keys into a new object (we simply create a view using the subList() method).
	 * Therefore, the original key set must not be destroyed after the split.
	 * 
	 * @return
	 */
	public Pair<CartilageIndexKeySet,CartilageIndexKeySet> splitInTwo(){
		CartilageIndexKeySet k1 = new CartilageIndexKeySet(values.subList(0, values.size()/2));
		CartilageIndexKeySet k2 = new CartilageIndexKeySet(values.subList(values.size()/2, values.size()));
		return new Pair<CartilageIndexKeySet,CartilageIndexKeySet>(k1,k2);
	}
	
	
	/**
	 * Sort and then split the key set. 
	 * @param attributeIdx
	 * @return
	 */
	public Pair<CartilageIndexKeySet,CartilageIndexKeySet> sortAndSplit(final int attributeIdx){
		sort(attributeIdx);
		return splitInTwo();
	}
	
	public List<Object[]> getValues(){
		return values;
	}
	
	public void reset(){
		values = Lists.newArrayList();
		types = null;
	}
	
	/**
	 * Iterate over the keys in the key set.
	 * @return
	 */
	public KeySetIterator iterator(){
		return new KeySetIterator(values);
	}
	
	/**
	 * An iterator to iterate over the key in a key set.
	 * The Iterator returns an instance of ParsedIndexKey which extends CartilageIndexKey.
	 * 
	 * @author alekh
	 *
	 */
	public static class KeySetIterator implements Iterator<CartilageIndexKey>{
		private Iterator<Object[]> valueItr;
		private ParsedIndexKey key;
		public KeySetIterator(List<Object[]> values){
			this.valueItr = values.iterator();
			key = new ParsedIndexKey();
		}
		public boolean hasNext() {
			return valueItr.hasNext();
		}
		public CartilageIndexKey next() {
			key.setValues(valueItr.next());
			return key;
		}
		public void remove() {
			next();
		}
	}
}
