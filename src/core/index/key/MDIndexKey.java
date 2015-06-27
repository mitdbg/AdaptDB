package core.index.key;

import core.utils.TypeUtils.SimpleDate;

/**
 * Could be created as template?
 * e.g. T for tuple type.
 * 
 * 
 * This class defines how the key is extracted from the tuple
 * 
 * @author alekh
 *
 */
public interface MDIndexKey {

	public void setTuple(Object tuple);
	
	String getKeyString();
	
	String getStringAttribute(int index, int maxSize);
	
	int getIntAttribute(int index);
	
	long getLongAttribute(int index);
	
	float getFloatAttribute(int index);
	
	double getDoubleAttribute(int index);
	
	SimpleDate getDateAttribute(int index);
}
