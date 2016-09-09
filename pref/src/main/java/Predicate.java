import java.util.ArrayList;
import java.util.List;


public class Predicate {
	public enum PREDTYPE {
		LEQ, GEQ, GT, LT, EQ
	};

	public int attribute;
	public TypeUtils.TYPE type;
	public Object value;
	public PREDTYPE predtype;
	public Schema schema;

	public Predicate(Schema schema, String attr, TypeUtils.TYPE t, Object val, PREDTYPE predtype) {
		this.schema = schema;
		this.attribute = schema.getAttributeId(attr);
		this.type = t;
		this.value = val;
		this.predtype = predtype;
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
		case LEQ:
			return value;
		case GEQ:
			return TypeUtils.deltaLess(value, type);

        case LT:
			if (type == TypeUtils.TYPE.INT || type == TypeUtils.TYPE.DATE)
				return TypeUtils.deltaLess(value, type);
			else
				return value;
		default:
			break;
		}
		return value;
	}
}
