package core.access.spark.join.algo;

import core.access.AccessMethod;
import core.access.spark.join.HPJoinInput;
import core.utils.Range;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class HyperJoinReplicatedBuckets extends JoinAlgo {

    HPJoinInput joinInput1;
    HPJoinInput joinInput2;

    public HyperJoinReplicatedBuckets(HPJoinInput joinInput1, HPJoinInput joinInput2) {
        this.joinInput1 = joinInput1;
        this.joinInput2 = joinInput2;
    }

    @Override
    public List<InputSplit> getSplits() {
        List<InputSplit> finalSplits = new ArrayList<InputSplit>();
        List<Tuple2<Range, int[]>> rangesToIds = joinInput2.getAssignedBucketSplits(SPLIT_FANOUT, false);
        for(Tuple2<Range, int[]> r : rangesToIds){
            // ids from smaller table that match this range of values
            AccessMethod.PartitionSplit[] splits = joinInput1.getRangeScan(true, r._1().getLow(), r._1().getHigh());

            Path[] input1Paths = joinInput1.getPaths(splits);
            Path[] input2Paths = joinInput2.getPaths(r._2());
            System.out.println("number of files from the smaller input: "+ input1Paths.length);
            System.out.println("number of files from the larger input: "+ input2Paths.length);

            long[] input1Lengths = joinInput1.getLengths(splits);
            long[] input2Lengths = joinInput2.getLengths(r._2());

            InputSplit thissplit = formSplit(input1Paths, input2Paths, input1Lengths, input2Lengths);
            finalSplits.add(thissplit);
        }
        return finalSplits;
    }
}
