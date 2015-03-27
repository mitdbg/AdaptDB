package core.adapt;

import core.utils.RangeUtils.Range;

public class Predicate {
	private int attribute;
	private Range range;

	public Predicate(int attr, Range r) {
		this.range = r;
		this.attribute = attr;
	}

	public int getAttribute() {
		return attribute;
	}

	public Range getRange() {
		return range;
	}
}
