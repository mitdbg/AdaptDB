package core.adapt.iterator;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import core.common.globals.Globals;
import core.common.index.JRNode;
import core.common.index.JoinRobustTree;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.io.Text;

import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;

import core.adapt.HDFSPartition;
import core.adapt.Partition;
import core.adapt.Query;
import core.common.index.RNode;
import core.common.index.RobustTree;
import core.utils.HDFSUtils;

/**
 * Created by ylu on 1/27/16.
 */


/**
 * Repartitions the input partitions and writes it out.
 * Does this by reading the new index. For each tuple, gets its new bucket id.
 * Writes it out the corresponding bucket.
 * Does not delete old partitions
 *
 * @author yil
 */
public class JoinRepartitionIterator extends PartitionIterator {

    private JRNode newIndexTree;
    protected String zookeeperHosts;
    private int indexPartition;

    protected Map<Integer, Partition> newPartitions = new HashMap<Integer, Partition>();

    public JoinRepartitionIterator() {
    }

    public JoinRepartitionIterator(String iteratorString) {
        try {
            readFields(ByteStreams.newDataInput(iteratorString.getBytes()));
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to read the fields");
        }
    }

    public JoinRepartitionIterator(Query query, int partition) {
        super(query);
        this.indexPartition = partition;
    }

    public void setZookeeper(String zookeeperHosts) {
        this.zookeeperHosts = zookeeperHosts;
    }

    public Query getQuery() {
        return this.query;
    }

    public int getIndexPartition(){
        return indexPartition;
    }

    public JRNode getIndexTree() {
        return this.newIndexTree;
    }

    /**
     * Gets a HDFS Partition as input.
     * Loads the new index. PartitionWriter::setPartition does the rest.
     */
    @Override
    public void setPartition(Partition partition) {
        super.setPartition(partition);
        if (newIndexTree == null) {
            String path = FilenameUtils.getPathNoEndSeparator(partition
                    .getPath());

            if (FilenameUtils.getBaseName(path).contains("partitions")
                    || FilenameUtils.getBaseName(path).contains("repartition")) { // hack
                path = FilenameUtils.getPathNoEndSeparator(FilenameUtils.getPath(path));
            }

            if (FilenameUtils.getBaseName(path).contains("data")) { // hack
                path = FilenameUtils.getPathNoEndSeparator(FilenameUtils.getPath(path));
            }

            String pathToIndex = path + "/index";

            if (indexPartition != -1){
                pathToIndex = pathToIndex + "." + indexPartition;
            }

            // Initialize JoinRobustTree.
            byte[] indexBytes = HDFSUtils.readFile(
                    ((HDFSPartition) partition).getFS(), pathToIndex);
            JoinRobustTree tree = new JoinRobustTree(Globals.getTableInfo(query.getTable()));
            tree.unmarshall(indexBytes);
            newIndexTree = tree.getRoot();
        }
    }

    @Override
    protected boolean isRelevant(IteratorRecord record) {
        int id = newIndexTree.getBucketId(record);

        Partition p;
        if (newPartitions.containsKey(id)) {
            p = newPartitions.get(id);
        } else {
            p = partition.clone();
            p.setPartitionId(id);
            newPartitions.put(id, p);
        }

        p.write(record.getBytes(), 0, record.getBytes().length);

        if (p.size() > 2 * 1024 * 1024 ){ // 2mb
            p.store(true);
            newPartitions.remove(id);
        }

        return query.qualifies(record);
    }

    @Override
    public void finish() {
        if (zookeeperHosts != null) {

            for (Partition p : newPartitions.values()) {
                System.out.println("storing partition id " + p.getPartitionId());
                p.store(true);
            }

            newPartitions = Maps.newHashMap();
        } else {
            System.out.println("INFO: Zookeeper Hosts NULL");
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        query.write(out);
        Text.writeString(out, zookeeperHosts);
        Text.writeString(out, Integer.toString(indexPartition));
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        String predicateString = Text.readString(in);
        query = new Query(predicateString);
        zookeeperHosts = Text.readString(in);
        indexPartition = Integer.parseInt(Text.readString(in));
    }

    public static JoinRepartitionIterator read(DataInput in) throws IOException {
        JoinRepartitionIterator it = new JoinRepartitionIterator();
        it.readFields(in);
        return it;
    }
}
