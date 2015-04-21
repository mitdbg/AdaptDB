package core.adapt;

import core.utils.SchemaUtils.TYPE;

public class Predicate {
	public enum PREDTYPE {LEQ, GEQ, GT, LT, EQ};
	public int attribute;
    public TYPE type;
    public Object value;
    public PREDTYPE predtype;

	public Predicate(int attr, TYPE t, Object val, PREDTYPE predtype) {
		this.attribute = attr;
		this.type = t;
		this.value = val;
		this.predtype = predtype;
	}
}
