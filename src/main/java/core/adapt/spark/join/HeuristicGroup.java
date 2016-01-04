package core.adapt.spark.join;

import java.util.*;

/**
 * Created by ylu on 1/4/16.
 */

public class HeuristicGroup implements JoinAlgo {

    Random rand = new Random();

    private int getIntersectionSize(HashSet<Integer> chunks, ArrayList<Integer> vals) {
        int sum = 0;
        for (int i = 0; i < vals.size(); i++) {
            if (chunks.contains(vals.get(i))) {
                sum++;
            }
        }
        return sum;
    }

    public ArrayList<ArrayList<Integer>> getSplits(int[] dataset, Map<Integer, ArrayList<Integer>> overlap, int budget){

        // Making things more deterministic.
        rand.setSeed(0);

        //System.out.println("Info: In getSplits");

        ArrayList<ArrayList<Integer> > final_splits = new ArrayList<ArrayList<Integer>>();

        LinkedList<Integer> pos = new LinkedList<Integer>();
        int size = dataset.length;
        for(int i = 0 ;i < dataset.length;i ++){
            pos.add(dataset[i]);
        }

        while(size > 0){
            ArrayList<Integer> cur_split = new  ArrayList<Integer>();
            int randPos = rand.nextInt(size);

            int start = pos.get(randPos);
            cur_split.add(start);
            pos.remove(randPos);
            size --;

            HashSet<Integer> chunks = new HashSet<Integer>();

            for(int rhs: overlap.get(start)){
                chunks.add(rhs);
            }


            for (int i = 1; i < budget && size > 0; i++) {
                int maxIntersection = -1;
                int best_offset = -1;

                ListIterator<Integer> it = pos.listIterator();
                int offset = 0;

                while(it.hasNext()){
                    int value = it.next();
                    if (maxIntersection == -1) {
                        maxIntersection = getIntersectionSize(chunks, overlap.get(value));
                        best_offset = offset;
                    } else {
                        int curIntersection = getIntersectionSize(chunks, overlap.get(value));
                        if (curIntersection > maxIntersection) {
                            maxIntersection = curIntersection;
                            best_offset = offset;
                        }
                    }
                    offset ++;
                }
                int value = pos.get(best_offset);
                cur_split.add(value);
                for(int rhs :  overlap.get(value)){
                    chunks.add(rhs);
                }
                pos.remove(best_offset);
                size --;
            }
            final_splits.add(cur_split);
        }
        return final_splits;
    }
}
