package perf.benchmark;

import core.adapt.Predicate;
import core.adapt.Query;

/**
 * Created by ylu on 12/14/15.
 */
public class PlayGround {
    public static void main(String[] args) {

        Query q = new Query(new Predicate[0]);
        String s =  q.toString();
        Query q1 = new Query(s);
        System.out.println();
    }
}
