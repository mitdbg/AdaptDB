package core.adapt.spark.join;

import java.util.*;

/**
 * Created by ylu on 12/3/15.
 */
public class RandomGroup implements JoinAlgo {
    public ArrayList<ArrayList<Integer>> getSplits(int[] dataset, Map<Integer, ArrayList<Integer>> overlap, int budget){

        //System.out.println("Info: In getSplits");

        ArrayList<Integer> perms = new ArrayList<Integer>();
        for(int i = 0 ;i < dataset.length; i ++){
            perms.add(dataset[i]);
        }
        Collections.shuffle(perms);
        ArrayList<ArrayList<Integer> > final_splits = new ArrayList<ArrayList<Integer>>();

        //System.out.println("Info: Barrier " + perms.size());

        for(int i = 0 ; i < perms.size(); i += budget){
            ArrayList<Integer> cur_split = new  ArrayList<Integer>();
            for(int j = 0; j < budget && j + i < perms.size(); j ++){
                cur_split.add(perms.get(i + j));
            }
            final_splits.add(cur_split);
        }
        //System.out.println("Info: Out getSplits");
        return final_splits;
    }
}
