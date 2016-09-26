import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


public class Predicate implements Serializable {
	private static final long serialVersionUID = 1L;

	public enum PREDTYPE {
		LEQ, GEQ, GT, LT, EQ
	};

	public int attribute;
	public TypeUtils.TYPE type;
	public Object value;
	public PREDTYPE predtype;

	public Predicate(Schema schema, String attr, TypeUtils.TYPE t, Object val, PREDTYPE predtype) {
		this.attribute = schema.getAttributeId(attr);
		this.type = t;
		this.value = val;
		this.predtype = predtype;
	}

	public Predicate(String predString) {
		String[] tokens = predString.split(":");
		this.attribute = Integer.parseInt(tokens[0]);
		this.type = TypeUtils.TYPE.valueOf(tokens[1]);
		this.value = TypeUtils.deserializeValue(this.type, tokens[2]);
		this.predtype = PREDTYPE.valueOf(tokens[3]);
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
		return attribute + ":" + type.toString() + ":"
				+ TypeUtils.serializeValue(value, type) + ":"
				+ predtype.toString();
	}
}
