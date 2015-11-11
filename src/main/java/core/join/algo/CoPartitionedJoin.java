package core.join.algo;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;

import core.join.HPJoinInput;

public class CoPartitionedJoin extends JoinAlgo {

	static int SPLIT_FANOUT = 5;

	HPJoinInput joinInput1;
	HPJoinInput joinInput2;

	public CoPartitionedJoin(HPJoinInput joinInput1, HPJoinInput joinInput2) {
		this.joinInput1 = joinInput1;
		this.joinInput2 = joinInput2;
	}

	@Override
	public List<InputSplit> getSplits() {
		List<InputSplit> finalSplits = new ArrayList<InputSplit>();
		List<int[]> splits = joinInput2.getEvenBucketSplits(SPLIT_FANOUT);
		for (int[] split : splits) {
			Path[] input1Paths = joinInput1.getPaths(split);
			Path[] input2Paths = joinInput2.getPaths(split);
			System.out.println("number of files from the smaller input: "
					+ input1Paths.length);
			System.out.println("number of files from the larger input: "
					+ input2Paths.length);

			long[] input1Lengths = joinInput1.getLengths(split);
			long[] input2Lengths = joinInput2.getLengths(split);

			InputSplit thissplit = formSplit(input1Paths, input2Paths,
					input1Lengths, input2Lengths);
			finalSplits.add(thissplit);
		}
		return finalSplits;
	}

}
