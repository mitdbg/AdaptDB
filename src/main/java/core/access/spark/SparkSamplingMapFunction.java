package core.access.spark;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;

/**
 * Created by qui on 5/21/15.
 */
public class SparkSamplingMapFunction implements
		FlatMapFunction<Iterator<String>, String> {
	private static final long serialVersionUID = 1L;
	private double samplingRate;

	public SparkSamplingMapFunction(double samplingRate) {
		this.samplingRate = samplingRate;
	}

	public Iterable<String> call(Iterator<String> stringIterator)
			throws Exception {
		List<String> result = new ArrayList<String>();
		if (Math.random() < samplingRate) {
			while (stringIterator.hasNext()) {
				result.add(stringIterator.next());
			}
		}
		return result;
	}
}
