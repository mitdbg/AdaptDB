package core.adapt;

import java.util.ArrayList;
import java.util.List;

public class Query {
	public static class FilterQuery extends Query{
		private List<Predicate> predicates;
		
		public FilterQuery() {
			this.predicates = new ArrayList<Predicate>();
		}
		
		public FilterQuery(List<Predicate> predicates) {
			this.predicates = predicates;
		}
		
		public void addPredicate(Predicate pred) {
			this.predicates.add(pred);
		}

		public List<Predicate> getPredicates() {
			return this.predicates;
		}
	}

	public class JoinQuery extends Query{
		//TODO:
	}
}
