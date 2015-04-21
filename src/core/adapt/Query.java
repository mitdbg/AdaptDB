package core.adapt;

import core.access.Predicate;


public class Query {
	public static class FilterQuery extends Query{
		private Predicate[] predicates;

		public FilterQuery() {
			this.predicates = null;
		}

		public FilterQuery(Predicate[] predicates) {
			this.predicates = predicates;
		}

		public Predicate[] getPredicates() {
			return this.predicates;
		}
	}

	public class JoinQuery extends Query{
		//TODO:
	}
}
