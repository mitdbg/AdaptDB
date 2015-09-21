package core.index.algo;

public class RangeTree {

	// public static class Node{
	// private Map<Range,Node> children;
	//
	//
	// }

	private int D; // dataset size
	private int P; // minimum partition size
	private int A; // number of attributes
	private int r; // number of heterogenous replicas

	public RangeTree(int D, int P, int A, int r) {
		this.D = D;
		this.P = P;
		this.A = A;
		this.r = r;
	}

	/**
	 * Get the number of attributes that could be packed in the tree, each with
	 * the given fanout and coverage.
	 * 
	 * a = r/c.logf (D/P)
	 * 
	 * @param f
	 *            - the fanout of the tree
	 * @param c
	 *            - the coverage of each attribute
	 * @return the number of attributes.
	 */
	public double getNumAttributes(int f, double c) {
		return (double) r / c * Math.log(D / P) / Math.log(f);
	}

	/**
	 * Get the fanout with the given coverage
	 * 
	 * @param c
	 *            -
	 * @return
	 */
	public double getFanout(double c) {
		return Math.pow((double) D / P, (double) r / c / A);
	}

	/**
	 * 
	 * @param l
	 *            -- the level
	 * @return
	 */
	public double getSlotsPerAttribute(int l) {
		double x = Math.pow(2, l - 1);
		return x * (Math.log((double) D / P) / Math.log(2) - l + 1)
				/ ((double) A / r - x + 1);
	}

	public static void main(String[] args) {
		int D = 8, P = 1, A = 3, r = 1;

		RangeTree tree = new RangeTree(D, P, A, r);

		for (int l = 1; l <= 3; l++) {
			System.out.println(tree.getSlotsPerAttribute(l));
		}

	}

}
