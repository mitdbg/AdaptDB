package core.access.spark;

import core.access.iterator.DistributedPartitioningIterator;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by qui on 5/20/15.
 */
class SparkPartitioningMapFunction implements FlatMapFunction<Iterator<String>, String> {

    private static final long serialVersionUID = 1L;
    private DistributedPartitioningIterator itr;

    public SparkPartitioningMapFunction(DistributedPartitioningIterator itr) {
        this.itr = itr;
    }

    @Override
    public Iterable<String> call(Iterator<String> stringIterator) throws Exception {
        itr.setIterator(stringIterator);
        while(itr.hasNext());
        itr.finish();
        return new ArrayList<String>();	//TODO: check
    }
}
