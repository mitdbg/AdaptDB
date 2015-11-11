package core.join.algo;

import core.adapt.iterator.ReusablePartitionIterator;
import core.adapt.spark.SparkInputFormat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;

import java.util.List;

/**
 * Created by qui on 7/22/15.
 */
public abstract class JoinAlgo {

	public static int SPLIT_FANOUT = 4;

	public abstract List<InputSplit> getSplits();

	public static InputSplit formSplit(Path[] paths1, Path[] paths2,
			long[] lengths1, long[] lengths2) {
		int totalLength = paths1.length + paths2.length;
		Path[] paths = new Path[totalLength];
		long[] lengths = new long[totalLength];
		for (int i = 0; i < totalLength; i++) {
			if (i < paths1.length) {
				paths[i] = paths1[i];
				lengths[i] = lengths1[i];
			} else {
				paths[i] = paths2[i - paths1.length];
				lengths[i] = lengths2[i - paths1.length];
			}
		}
		return new SparkInputFormat.SparkFileSplit(paths, lengths,
				new ReusablePartitionIterator());
	}

}
