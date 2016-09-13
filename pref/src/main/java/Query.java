
import java.io.Serializable;

import com.google.common.base.Joiner;

public class Query implements Serializable {

	private static final long serialVersionUID = 1L;

	private Predicate[] predicates;



	public Query(Predicate[] predicates) {
		this.predicates = predicates;
	}

	public Query(String queryString) {
		if (queryString.length() > 0) {
			String[] parts = queryString.split(";");
			this.predicates = new Predicate[parts.length];
			for (int i = 0; i < parts.length; i++) {
				this.predicates[i] = new Predicate(parts[i]);
			}
		} else {
			this.predicates = new Predicate[0];
		}

	}

	public String getStringAttribute(String attr) {
		return attr;
	}

	public int getIntAttribute(String attr) {
		return Integer.parseInt(attr);
	}

	public long getLongAttribute(String attr) {
		return Long.parseLong(attr);
	}

	public double getDoubleAttribute(String attr) {
		return Double.parseDouble(attr);
	}

	/*
     * Parse date assuming the format: "yyyy-MM-dd".
     * Skips anything after that.
     */
	public TypeUtils.SimpleDate getDateAttribute(String attr) {
		int year = 1000 * (attr.charAt(0) - '0') + 100 * (attr.charAt(1)  - '0')
				+ 10 * (attr.charAt(2) - '0') + (attr.charAt(3)  - '0');
		int month = 10 * (attr.charAt(5) - '0') + (attr.charAt(6)  - '0');
		int day = 10 * (attr.charAt(8)  - '0') + (attr.charAt(9) - '0');

		TypeUtils.SimpleDate date = new TypeUtils.SimpleDate(year, month, day);

		return date;
	}

	/**
	 * Assumes that the boolean data is represented as a single character in the
	 * ascii file.
	 *
	 */
	public boolean getBooleanAttribute(String attr) {

		if (attr.equals("1") || attr.equals("t"))
			return true;
		else if (attr.equals("0") || attr.equals("f"))
			return false;
		else
			throw new RuntimeException("Cannot parse the boolean attribute: " + attr);
	}

	public boolean qualifies(String record) {

		String[] attr = record.split(Global.SPLIT_DELIMITER);

		boolean qualify = true;
		for (Predicate p : predicates) {
			int attrIdx = p.attribute;
			switch (p.type) {
			case BOOLEAN:
				qualify &= p.isRelevant(getBooleanAttribute(attr[attrIdx]));
				break;
			case INT:
				qualify &= p.isRelevant(getIntAttribute(attr[attrIdx]));
				break;
			case LONG:
				qualify &= p.isRelevant(getLongAttribute(attr[attrIdx]));
				break;
			case DOUBLE:
				qualify &= p.isRelevant(getDoubleAttribute(attr[attrIdx]));
				break;
			case DATE:
				qualify &= p.isRelevant(getDateAttribute(attr[attrIdx]));
				break;
			case STRING:
				qualify &= p.isRelevant(getStringAttribute(attr[attrIdx]));
				break;
			case VARCHAR:
				qualify &= p.isRelevant(getStringAttribute(attr[attrIdx]));
				break;
			default:
				throw new RuntimeException("Invalid data type!");
			}
		}
		return qualify;
	}

	@Override
	public String toString() {
		String stringPredicates = "";
		if (predicates.length != 0)
			stringPredicates = Joiner.on(";").join(predicates);
		return stringPredicates;
	}
}
