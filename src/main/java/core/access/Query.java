package core.access;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.Text;

import com.google.common.base.Joiner;

import core.access.iterator.IteratorRecord;
import core.globals.Globals;
import core.key.RawIndexKey;

public class Query implements Serializable {
	private static final long serialVersionUID = 1L;

	private static final String EMPTY = "empty";

	protected Predicate[] predicates;
	protected RawIndexKey key;

	public Predicate[] getPredicates() {
		return this.predicates;
	}

	public void write(DataOutput out) throws IOException {
		if (predicates.length == 0) {
			Text.writeString(out, EMPTY);
		} else {
			Text.writeString(out, Joiner.on(";").join(predicates));
		}
		Text.writeString(out, key.toString());
	}

	public void readFields(DataInput in) throws IOException {
		String predicateStrings = Text.readString(in);
		if (predicateStrings.equals(EMPTY)) {
			predicates = new Predicate[0];
		} else {
			String[] tokens = predicateStrings.split(";");
			predicates = new Predicate[tokens.length];
			for (int i = 0; i < predicates.length; i++)
				predicates[i] = new Predicate(tokens[i]);
		}
		key = new RawIndexKey(Text.readString(in));
	}

	public static class FilterQuery extends Query implements Serializable {
		private static final long serialVersionUID = 1L;

		public FilterQuery() {
			this.predicates = null;
			key = new RawIndexKey(Globals.DELIMITER);
		}

		public FilterQuery(String predString) {
			String[] parts = predString.split(";");
			this.predicates = new Predicate[parts.length - 1];
			for (int i = 0; i < parts.length - 1; i++) {
				this.predicates[i] = new Predicate(parts[i]);
			}
		}

		public FilterQuery(Predicate[] predicates, RawIndexKey key) {
			this.predicates = predicates;
			this.key = key;
		}

		public FilterQuery(Predicate[] predicates) {
			this(predicates, new RawIndexKey(Globals.DELIMITER));
		}

		public boolean qualifies(IteratorRecord record) {
			boolean qualify = true;
			for (Predicate p : predicates) {
				int[] keyAttrs = key.getKeys();
				int attrIdx;
				if (keyAttrs == null) {
					attrIdx = p.attribute;
				} else {
					attrIdx = keyAttrs[p.attribute];
				}
				switch (p.type) {
				case BOOLEAN:
					qualify &= p
							.isRelevant(record.getBooleanAttribute(attrIdx));
					break;
				case INT:
					qualify &= p.isRelevant(record.getIntAttribute(attrIdx));
					break;
				case LONG:
					qualify &= p.isRelevant(record.getLongAttribute(attrIdx));
					break;
				case DOUBLE:
					qualify &= p.isRelevant(record.getFloatAttribute(attrIdx));
					break;
				case DATE:
					qualify &= p.isRelevant(record.getDateAttribute(attrIdx));
					break;
				case STRING:
					qualify &= p.isRelevant(record.getStringAttribute(attrIdx,
							20));
					break;
				case VARCHAR:
					qualify &= p.isRelevant(record.getStringAttribute(attrIdx,
							100));
					break;
				default:
					throw new RuntimeException("Invalid data type!");
				}
			}
			return qualify;
		}

		@Override
		public String toString() {
			return Joiner.on(";").join(predicates);
		}

		public static FilterQuery read(DataInput in) throws IOException {
			FilterQuery q = new FilterQuery();
			q.readFields(in);
			return q;
		}
	}

	public class JoinQuery extends Query {
		private static final long serialVersionUID = 1L;
	}
}
