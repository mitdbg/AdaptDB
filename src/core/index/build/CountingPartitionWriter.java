package core.index.build;

import java.io.OutputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import core.index.MDIndex;
import core.utils.ConfUtils;

/**
 * Created by qui on 3/30/15.
 */
public class CountingPartitionWriter extends PartitionWriter {

    Map<String, Integer> bucketCounts = new HashMap<String, Integer>();
    ConfUtils conf;

    public CountingPartitionWriter(String partitionDir, String propertiesFile){
        super(partitionDir);
        conf = new ConfUtils(propertiesFile);
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

    @Override
    public void flush() {
    	MDIndex.BucketCounts c = new MDIndex.BucketCounts(conf.getZOOKEEPER_HOSTS());
        //CuratorFramework client = c.getClient();
        //String lockPathBase = "/partition-lock-";
        Iterator<Map.Entry<String, Integer>> entries = bucketCounts.entrySet().iterator();
        while (entries.hasNext()) {
            Map.Entry<String, Integer> e = entries.next();
            try {
            	//InterProcessSemaphoreMutex lock = CuratorUtils.acquireLock(client, lockPathBase + e.getKey());
                c.addToBucketCount(Integer.parseInt(e.getKey()), e.getValue());
                entries.remove();
                //CuratorUtils.releaseLock(lock);
            } catch (NumberFormatException ex) {

            }
        }
        c.close();
    }
}
