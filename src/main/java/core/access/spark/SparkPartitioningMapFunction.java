package core.access.spark;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;

import core.access.iterator.DistributedPartitioningIterator;

/**
 * Created by qui on 5/20/15.
 */
class SparkPartitioningMapFunction implements
		FlatMapFunction<Iterator<String>, String> {

	private static final long serialVersionUID = 1L;
	private DistributedPartitioningIterator itr;

	public SparkPartitioningMapFunction(DistributedPartitioningIterator itr) {
		this.itr = itr;
	}

	public Iterable<String> call(Iterator<String> stringIterator)
			throws Exception {
		itr.setIterator(stringIterator);
		while (itr.hasNext())
			;
		itr.finish();
		return new ArrayList<String>(); // TODO: check
	}
}
