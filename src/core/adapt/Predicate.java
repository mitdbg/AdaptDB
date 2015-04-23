package core.adapt;

import core.utils.SchemaUtils.TYPE;
import core.utils.TypeUtils;

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

	/**
	 * Check if tuple with value for attribute is accepted (true) or rejected (false) by predicate
	 * @param value
	 * @return
	 */
	public boolean isRelevant(Object value) {
		switch (this.predtype) {
		case GEQ:
			if (TypeUtils.compareTo(this.value, value, this.type) <= 0) return true;
			break;
		case LEQ:
			if (TypeUtils.compareTo(this.value, value, this.type) >= 0) return true;
			break;
		case GT:
			if (TypeUtils.compareTo(this.value, value, this.type) < 0) return true;
			break;
		case LT:
			if (TypeUtils.compareTo(this.value, value, this.type) < 0) return true;
			break;
		case EQ:
			if (TypeUtils.compareTo(this.value, value, this.type) == 0) return true;
		}

		return false;
	}
}
