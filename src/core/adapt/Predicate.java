package core.adapt;

import core.utils.RangeUtils.Range;
import core.utils.Schema.Attribute;

public class Predicate {
	private Attribute attribute;
	private Range range;

	public Predicate(Attribute attr, Range r) {
		this.range = r;
		this.attribute = attr;
	}
	
	public Attribute getAttribute() {
		return attribute;
	}

	public Range getRange() {
		return range;
	}
}
