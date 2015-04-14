package core.adapt;

import core.utils.SchemaUtils.TYPE;

public class Predicate {
	public int attribute;
    public TYPE type;
    public Object value;

	public Predicate(int attr, TYPE t, Object val) {
		this.attribute = attr;
		this.type = t;
		this.value = val;
	}
}
