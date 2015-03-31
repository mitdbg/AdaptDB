package core.index.build;

import core.utils.TreeUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by qui on 3/30/15.
 */
public class CountingPartitionWriter extends PartitionWriter {

    Map<String, Integer> bucketCounts = new HashMap<String, Integer>();

    public CountingPartitionWriter(String partitionDir){
        super(partitionDir);
    }

    @Override
    public void writeToPartition(String partitionId, byte[] bytes, int b_offset, int b_length){
        if (!bucketCounts.containsKey(partitionId)) {
            bucketCounts.put(partitionId, 1);
        } else {
            int oldCount = bucketCounts.get(partitionId);
            bucketCounts.put(partitionId, oldCount + 1);
        }
    }

    public Map<String, Integer> getCounts() {
        return bucketCounts;
    }

    @Override
    protected OutputStream getOutputStream(String path) {
        return null;
    }

    @Override
    public void createPartitionDir() {

    }
}
