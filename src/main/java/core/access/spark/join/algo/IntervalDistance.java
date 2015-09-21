package core.access.spark.join.algo;

import org.apache.commons.math3.ml.distance.DistanceMeasure;

/**
 * Created by qui on 7/29/15.
 */
public class IntervalDistance implements DistanceMeasure {

	private static final long serialVersionUID = 5702558222518414334L;

	public double compute(double[] doubles, double[] doubles1) {
		// self.high > other.low and self.low < other.high
		if (doubles[1] > doubles1[0] && doubles[0] < doubles1[1]) {
			double interval1 = doubles[1] - doubles1[0];
			double interval2 = doubles1[1] - doubles[0];
			if (interval1 <= interval2) {
				return 1 - interval1 / interval2;
			} else {
				return 1 - interval2 / interval1;
			}
		} else {
			return 1;
		}
	}
}
