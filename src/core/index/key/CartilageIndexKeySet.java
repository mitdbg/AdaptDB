package core.index.key;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Lists;

import core.utils.Pair;
import core.utils.RangeUtils.SimpleDateRange.SimpleDate;
import core.utils.SchemaUtils.TYPE;
import core.utils.TypeUtils;

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
	public CartilageIndexKeySet(List<Object[]> values, TYPE[] types) {
        this.values = values;
        this.types = types;
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
	 * Return the last entry in the keyset
	 */
	public Object getLast(int dim) {
		assert values.size() > 0;
		assert dim < types.length;
		return values.get(values.size() - 1)[dim];
	}

	// TODO: Make this compatible with the one in TypeUtils
	public Comparator<Object[]> getComparatorForType(TYPE type, final int attributeIdx) {
		switch(type){
		case INT:
			return new Comparator<Object[]> (){
				public int compare(Object[] o1, Object[] o2) {
					return ((Integer)o1[attributeIdx]).compareTo((Integer)o2[attributeIdx]);
				}
			};
		case LONG:
			return new Comparator<Object[]> (){
				public int compare(Object[] o1, Object[] o2) {
					return ((Long)o1[attributeIdx]).compareTo((Long)o2[attributeIdx]);
				}
			};
		case FLOAT:
			return new Comparator<Object[]> (){
				public int compare(Object[] o1, Object[] o2) {
					return ((Float)o1[attributeIdx]).compareTo((Float)o2[attributeIdx]);
				}
			};
		case DATE:
			return new Comparator<Object[]> (){
				public int compare(Object[] o1, Object[] o2) {
					return ((SimpleDate)o1[attributeIdx]).compareTo((SimpleDate)o2[attributeIdx]);
				}
			};
		case STRING:
			return new Comparator<Object[]> (){
				public int compare(Object[] o1, Object[] o2) {
					return ((String)o1[attributeIdx]).compareTo((String)o2[attributeIdx]);
				}
			};
		case VARCHAR:
			throw new RuntimeException("sorting over varchar is not supported"); // skip partitioning on varchar attribute
		default:
			throw new RuntimeException("Unknown dimension type: "+ type);
		}
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
		Collections.sort(values, this.getComparatorForType(sortType, attributeIdx));
	}

	/**
	 * Split this key set into two equal sized key sets.
     * If the length is odd, the extra value goes in the second set.
	 * This function assumes that the key set has already been sorted.
	 *
	 * Note that we do not copy the keys into a new object (we simply create a view using the subList() method).
	 * Therefore, the original key set must not be destroyed after the split.
	 *
	 * @return
	 */
	public Pair<CartilageIndexKeySet,CartilageIndexKeySet> splitInTwo(){
		CartilageIndexKeySet k1 = new CartilageIndexKeySet(values.subList(0, values.size()/2), types);
		CartilageIndexKeySet k2 = new CartilageIndexKeySet(values.subList(values.size()/2, values.size()), types);
		return new Pair<CartilageIndexKeySet,CartilageIndexKeySet>(k1,k2);
	}

    /**
     * Split this key set by the median on the specified attribute.
     * All keys with the specified attribute equal to the median go in the second set.
     * @return
     */
    public Pair<CartilageIndexKeySet,CartilageIndexKeySet> splitByMedian(int attributeIdx) {
        Object medianVal = values.get(values.size()/2)[attributeIdx];
        int firstIndex = getFirstIndexOfAttributeVal(attributeIdx, types[attributeIdx], medianVal, values, 0);
        CartilageIndexKeySet k1 = new CartilageIndexKeySet(values.subList(0, firstIndex), types);
        CartilageIndexKeySet k2 = new CartilageIndexKeySet(values.subList(firstIndex, values.size()), types);
        return new Pair<CartilageIndexKeySet,CartilageIndexKeySet>(k1,k2);
    }

    /**
     * Split this key set by the value on the specified attribute.
     * @return
     */
    public Pair<CartilageIndexKeySet,CartilageIndexKeySet> splitAt(int attributeIdx, Object value) {
		final TYPE sortType = types[attributeIdx];

		Comparator<Object> comp = TypeUtils.getComparatorForType(sortType);

		// Finds the least k such that k >= value
        int lo = 0;
        int hi = values.size() - 1;
        while (lo < hi) {
            int mid = lo + (hi - lo) / 2;
        	Object midVal = this.values.get(mid)[attributeIdx];

            if (comp.compare(value, midVal) < 0) hi = mid;
            else lo = mid + 1;
        }

        CartilageIndexKeySet k1 = new CartilageIndexKeySet(values.subList(0, lo), types);
        CartilageIndexKeySet k2 = new CartilageIndexKeySet(values.subList(lo, values.size()), types);
        return new Pair<CartilageIndexKeySet,CartilageIndexKeySet>(k1,k2);
    }

    private static int getFirstIndexOfAttributeVal(int attributeIdx, TYPE type, Object val, List<Object[]> sublist, int start) {
        if (sublist.size() == 0) {
            return -1;
        }
        Object middle = sublist.get(sublist.size()/2)[attributeIdx];
        int comparison;
        switch(type) {
            case INT:       comparison = ((Integer)middle).compareTo((Integer)val); break;
            case LONG:		comparison = ((Long)middle).compareTo((Long)val); break;
            case FLOAT:		comparison = ((Float)middle).compareTo((Float)val); break;
            case DATE:		comparison = ((SimpleDate)middle).compareTo((SimpleDate)val); break;
            case STRING:	comparison = ((String)middle).compareTo((String)val); break;
            case VARCHAR:	throw new RuntimeException("sorting over varchar is not supported"); // skip partitioning on varchar attribute
            default:		throw new RuntimeException("Unknown dimension type: "+type);
        }
        if (comparison == 0) {
            int firstIndex = getFirstIndexOfAttributeVal(attributeIdx, type, val, sublist.subList(0,sublist.size()/2), start);
            if (firstIndex == -1) {
                return start + sublist.size()/2;
            } else {
                return firstIndex;
            }
        } else if (comparison > 0) {
            return getFirstIndexOfAttributeVal(attributeIdx, type, val, sublist.subList(0,sublist.size()/2), start);
        } else {
            return getFirstIndexOfAttributeVal(attributeIdx, type, val, sublist.subList(sublist.size()/2+1, sublist.size()), start+sublist.size()/2+1);
        }
    }


	/**
	 * Sort and then split the key set.
	 * @param attributeIdx
	 * @return
	 */
	public Pair<CartilageIndexKeySet,CartilageIndexKeySet> sortAndSplit(final int attributeIdx){
		sort(attributeIdx);
		return splitByMedian(attributeIdx);
	}

	public List<Object[]> getValues(){
		return values;
	}

	public void addValues(List<Object[]> e) {
		this.values.addAll(e);
	}

 	public void setTypes(TYPE[] types) {
		this.types = types;
	}

	public TYPE[] getTypes() {
		return this.types;
	}

    public int size() { return values.size(); }

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
