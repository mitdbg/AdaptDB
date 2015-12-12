package core.common.key;

import com.google.common.base.Joiner;

import core.common.globals.Globals;
import core.utils.TypeUtils.SimpleDate;
import core.utils.TypeUtils.TYPE;

public class ParsedIndexKey {

	private Object[] values;
	public TYPE[] types;

	public ParsedIndexKey(TYPE[] types) {
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
