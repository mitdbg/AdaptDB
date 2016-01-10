package core.adapt.spark.join;


import java.io.IOException;
import java.util.Iterator;

import com.google.common.collect.ArrayListMultimap;
import core.adapt.spark.SparkQueryConf;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import core.adapt.HDFSPartition;
import core.adapt.iterator.IteratorRecord;
import core.adapt.iterator.PartitionIterator;
import core.adapt.spark.join.SparkJoinInputFormat.SparkJoinFileSplit;
import core.utils.CuratorUtils;

/**
 * Created by ylu on 12/2/15.
 */


public class SparkJoinRecordReader extends
        RecordReader<LongWritable, Text> {

    protected Configuration conf;

    protected SparkJoinFileSplit sparkSplit;

    private int currentFile;

    private int tupleCountInTable1, tupleCountInTable2; // test only

    private String dataset1, dataset2;
    private int join_attr1, join_attr2;
    private int partitionKey;
    private String delimiter, splitter;

    protected PartitionIterator iter1;
    protected PartitionIterator iter2;

    ArrayListMultimap<Integer, byte[]> hashTable;

    Iterator<byte[]> firstRecords;
    byte[] secondRecord;

    LongWritable key;
    Text value;


    boolean hasNext;


    CuratorFramework client;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        conf = context.getConfiguration();
        client = CuratorUtils.createAndStartClient(conf
                .get(SparkQueryConf.ZOOKEEPER_HOSTS));

        sparkSplit = (SparkJoinFileSplit) split;


        join_attr1 = Integer.parseInt(conf.get("JOIN_ATTR1"));
        join_attr2 = Integer.parseInt(conf.get("JOIN_ATTR2"));

        partitionKey = Integer.parseInt(conf.get("PARTITION_KEY"));

        dataset1 = conf.get("DATASET1");
        dataset2 = conf.get("DATASET2");

        tupleCountInTable1 = 0;
        tupleCountInTable2 = 0;

        delimiter =  conf.get("DELIMITER");
        if (delimiter.equals("|"))
            splitter = "\\|";
        else
            splitter = delimiter;

        hashTable = ArrayListMultimap.create();
        iter1 = sparkSplit.getFirstIterator();
        iter2 = sparkSplit.getSecondIterator();


        key = new LongWritable();
        value = new Text();

        Path[] paths = sparkSplit.getPaths();

        System.out.println("[Info] chunks to join");
        for(int i = 0; i< paths.length; i ++){
            System.out.println(paths[i].toString());
        }


        // build hashtable

        build_hashtable();

        //System.out.println("the file is : "+  sparkSplit.getPath(currentFile));
        setPartitionToIterator(sparkSplit.getPath(currentFile), iter2);

        //System.out.println("There are " + hashTable.size() + " records!");

    }

    void setPartitionToIterator(Path path, PartitionIterator it) {
        FileSystem fs = null;
        try {
            fs = path.getFileSystem(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Loading " + path.toString() + " currentFile " + currentFile);
        HDFSPartition partition = new HDFSPartition(fs, path.toString(),
                Short.parseShort(conf.get(SparkQueryConf.HDFS_REPLICATION_FACTOR)),
                client);
        partition.loadNext(); // ???
        it.setPartition(partition);
    }

    private void build_hashtable() {

        for (currentFile = 0; currentFile < sparkSplit.getNumPaths(); currentFile++) {

            //System.out.println("TESTING: " + sparkSplit.getPath(currentFile).toString()  + " does it contain " + dataset2);

            if (sparkSplit.getPath(currentFile).toString().contains(dataset2 + "/")) { // solve the issue that dataset names share the same prefix

                //System.out.println("NEXT PATH should be " + sparkSplit.getPath(currentFile));
                break;
            }

            setPartitionToIterator(sparkSplit.getPath(currentFile), iter1);

            while (iter1.hasNext()) {

                tupleCountInTable1++;

                IteratorRecord r = iter1.next();
                byte[] rawBytes = r.getBytes();
                int key = r.getIntAttribute(join_attr1);
                hashTable.put(key, rawBytes);
            }
        }

    }

    private void getNext() {

        if (firstRecords != null && firstRecords.hasNext()) {
            hasNext = true;
        } else {
            while (true) {
                if (iter2.hasNext()) {
                    IteratorRecord r = iter2.next();
                    int key = r.getIntAttribute(join_attr2);

                    tupleCountInTable2++;

                    if (hashTable.containsKey(key)) {
                        firstRecords = hashTable.get(key).iterator();
                        secondRecord = r.getBytes();
                        hasNext = true;

                        break;
                    }
                } else {
                    currentFile++;
                    if (currentFile >= sparkSplit.getNumPaths()) {
                        hasNext = false;
                        break;
                    } else {
                        setPartitionToIterator(sparkSplit.getPath(currentFile), iter2);
                    }
                }
            }
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        getNext();

        if (hasNext) {
            byte[] firstRecord = firstRecords.next();
            String part1 = new String(firstRecord, 0, firstRecord.length);
            String part2 = new String(secondRecord, 0, secondRecord.length);
            String strValue = part1 + delimiter + part2;
            key.set(Long.parseLong(strValue.split(splitter)[partitionKey]));
            value.set(strValue);
        }

        return hasNext;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException,
            InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException,
            InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return (float) currentFile / sparkSplit.getStartOffsets().length;
    }

    @Override
    public void close() throws IOException {
        iter2.finish(); // this method could even be called earlier in case
        // the entire split does not fit in main-memory
        // counter.close();
        // locker.cleanup();
        System.out.println("There are " + tupleCountInTable1 + " tuples from the first input!");
        System.out.println("There are " + tupleCountInTable2 + " tuples from the second input!");
    }
}
