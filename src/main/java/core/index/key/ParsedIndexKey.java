package core.index.key;

import com.google.common.base.Joiner;

import core.utils.TypeUtils.*;

public class ParsedIndexKey extends CartilageIndexKey {

	private Object[] values;

	public ParsedIndexKey(TYPE[] types) {
		super('|'); // some dummy delimiter (not used really)
		this.types = types;
	}

	public void setValues(Object[] values) {
		this.values = values;
	}

	public String getKeyString() {
		return Joiner.on(",").join(values);
	}

	public String getStringAttribute(int index, int maxSize) {
		return (String) values[index];
	}

	public int getIntAttribute(int index) {
		return (Integer) values[index];
	}

	public long getLongAttribute(int index) {
		return (Long) values[index];
	}

	public float getFloatAttribute(int index) {
		return (Float) values[index];
	}

	public double getDoubleAttribute(int index) {
		return (Double) values[index];
	}

	public SimpleDate getDateAttribute(int index) {
		return (SimpleDate) values[index];
	}

	public SimpleDate getDateAttribute(int index, SimpleDate date) {
		return getDateAttribute(index);
	}

	public boolean getBooleanAttribute(int index) {
		return (Boolean) values[index];
	}
}
