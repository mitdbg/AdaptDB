package core.adapt.spark;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by ylu on 12/3/15.
 */

public interface JoinAlgo {
    public ArrayList<ArrayList<Integer>> getSplits(int[] dataset, Map<Integer, ArrayList<Integer>> overlap, int budget);


}
