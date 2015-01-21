package core.index.key;

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
	
	String getStringAttribute(int index);
	
	int getIntAttribute(int index);
	
	long getLongAttribute(int index);
	
	float getFloatAttribute(int index);
	
	double getDoubleAttribute(int index);
}
