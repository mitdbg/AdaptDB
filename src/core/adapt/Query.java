package core.adapt;

import core.utils.RangeUtils.Range;
import core.utils.Schema.Attribute;

public class Query {

	
	
	public static class FilterQuery extends Query{

		private Attribute filterAttribute;
		private Range filterRange;
		
		public FilterQuery(Attribute filterAttribute, Range filterRange){
			this.filterAttribute = filterAttribute;
			this.filterRange = filterRange;
		}
		
		public Attribute getFilterAttribute() {
			return filterAttribute;
		}

		public Range getFilterRange() {
			return filterRange;
		}
		
	}

	public class JoinQuery extends Query{
		//TODO:
	}
}
