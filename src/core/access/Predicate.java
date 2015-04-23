package core.access;

import core.utils.RangeUtils;
import core.utils.RangeUtils.Range;

public class Predicate {
	private int attribute;
	private Range range;
	
	public Predicate(String predicateString) {
		String[] tokens = predicateString.split("=");
		this.attribute = Integer.parseInt(tokens[0]);
		this.range = RangeUtils.parse(tokens[1]);		
	}
	
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
	
	public String toString(){
		return attribute +"="+ RangeUtils.toString(range);
	}
}
