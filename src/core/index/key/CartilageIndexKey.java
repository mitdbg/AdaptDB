package core.index.key;

import core.data.CartilageDatum.CartilageBinaryRecord;

public class CartilageIndexKey implements MDIndexKey{

	private CartilageBinaryRecord record;
	private int[] keyAttrIdx;
	
	public CartilageIndexKey(int[] keyAttrIdx){
		this.keyAttrIdx = keyAttrIdx;
	}
	
	public void setTuple(Object tuple) {
		this.record = (CartilageBinaryRecord)tuple;
	}

	public String getKeyString() {
		String keyString = "";
		for(int idx: keyAttrIdx){
			keyString += record.getValue(idx)+",";
		}
		return keyString;
	}

	public String getStringAttribute(int index) {
		return record.getValue(keyAttrIdx[index]);
	}

	public int getIntAttribute(int index) {
		return record.getIntValue(keyAttrIdx[index]);
	}

	public long getLongAttribute(int index) {
		return record.getLongValue(keyAttrIdx[index]);
	}

	public float getFloatAttribute(int index) {
		return record.getFloatValue(keyAttrIdx[index]);
	}
	
	public double getDoubleAttribute(int index) {
		return record.getDoubleValue(keyAttrIdx[index]);
	}
}
