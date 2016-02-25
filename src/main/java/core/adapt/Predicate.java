package core.adapt;

import core.common.globals.TableInfo;

import core.utils.TypeUtils;
import core.utils.TypeUtils.TYPE;

public class Predicate {
	public enum PREDTYPE {
		LEQ, GEQ, GT, LT, EQ
	};

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

	public Predicate(TableInfo tableInfo, String attr, TYPE t, Object val, PREDTYPE predtype) {
		this.attribute = tableInfo.schema.getAttributeId(attr);
		this.type = t;
		this.value = val;
		this.predtype = predtype;
	}

	public Predicate(String predString) {
		String[] tokens = predString.split(":");
		this.attribute = Integer.parseInt(tokens[0]);
		this.type = TYPE.valueOf(tokens[1]);
		this.value = TypeUtils.deserializeValue(this.type, tokens[2]);
		this.predtype = PREDTYPE.valueOf(tokens[3]);
	}

	public void normalizePredicate() {
		if (this.predtype == PREDTYPE.GEQ) {
			this.predtype = PREDTYPE.GT;
			this.value = getHelpfulCutpoint();
		} else if (this.predtype == PREDTYPE.LT) {
			this.predtype = PREDTYPE.LEQ;
			this.value = getHelpfulCutpoint();
		}
	}

	/**
	 * Check if tuple with value for attribute is accepted (true) or rejected
	 * (false) by predicate
	 *
	 * @param value
	 * @return
	 */
	public boolean isRelevant(Object value) {
		switch (this.predtype) {
		case GEQ:
			if (TypeUtils.compareTo(this.value, value, this.type) <= 0)
				return true;
			break;
		case LEQ:
			if (TypeUtils.compareTo(this.value, value, this.type) >= 0)
				return true;
			break;
		case GT:
			if (TypeUtils.compareTo(this.value, value, this.type) < 0)
				return true;
			break;
		case LT:
			if (TypeUtils.compareTo(this.value, value, this.type) > 0)
				return true;
			break;
		case EQ:
			if (TypeUtils.compareTo(this.value, value, this.type) == 0)
				return true;
		}

		return false;
	}

	@Override
	public String toString() {
		return "" + attribute + ":" + type.toString() + ":"
				+ TypeUtils.serializeValue(value, type) + ":"
				+ predtype.toString();
	}

	public Object getHelpfulCutpoint() {
		switch (this.predtype) {
		case EQ:
		case GT:
		case LT:
		case LEQ:
			return value;

			// TODO: LT is still wrong in this implementation
			// Avoid using LT anywhere in the evaluation
		case GEQ:
			return TypeUtils.deltaLess(value, type);
		default:
			break;
		}
		return value;
	}
}
