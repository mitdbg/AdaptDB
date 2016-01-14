package core.adapt.spark.join;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import core.adapt.AccessMethod.PartitionSplit;
import core.adapt.Query;
import core.adapt.iterator.*;
import core.adapt.spark.SparkQueryConf;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import core.utils.ReflectionUtils;

/**
 * Created by ylu on 12/2/15.
 */


public class SparkJoinInputFormat extends
        FileInputFormat<LongWritable, Text> implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Log LOG = LogFactory.getLog(FileInputFormat.class);

    private Configuration conf;
    private SparkQueryConf queryConf;

    private String dataset1,dataset2;
    private Query dataset1_query,dataset2_query;

    public static class SparkJoinFileSplit extends CombineFileSplit implements
            Serializable {
        private static final long serialVersionUID = 1L;

        private PartitionIterator iter1, iter2;

        public SparkJoinFileSplit() {
        }

        public SparkJoinFileSplit(Path[] files, long[] lengths,
                                  PartitionIterator iter1, PartitionIterator iter2) {
            super(files, lengths);
            this.iter1 = iter1;
            this.iter2 = iter2;
        }

        public PartitionIterator getFirstIterator() {
            return this.iter1;
        }
        public PartitionIterator getSecondIterator() {
            return this.iter2;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            super.write(out);
            Text.writeString(out, iter1.getClass().getName());
            iter1.write(out);
            iter2.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            super.readFields(in);
            String type = Text.readString(in);
            iter1 = (PartitionIterator) ReflectionUtils.getInstance(type);
            iter1.readFields(in);
            iter2 = (PartitionIterator) ReflectionUtils.getInstance(type);
            iter2.readFields(in);
        }

        public static SparkJoinFileSplit read(DataInput in) throws IOException {
            SparkJoinFileSplit s = new SparkJoinFileSplit();
            s.readFields(in);
            return s;
        }
    }

    private SparkJoinFileSplit formSplits(Path[] paths1, long[] lengths1, Path[] paths2, long[] lengths2, PartitionIterator iter1,PartitionIterator iter2 ) {
        int totalLength = paths1.length + paths2.length;
        Path[] paths = new Path[totalLength];
        long[] lengths = new long[totalLength];
        for (int i = 0; i < totalLength; i++) {
            if (i < paths1.length) {
                paths[i] = paths1[i];
                lengths[i] = lengths1[i];
            } else {
                paths[i] = paths2[i - paths1.length];
                lengths[i] = lengths2[i - paths1.length];
            }
        }
        return new SparkJoinFileSplit(paths, lengths, iter1, iter2);
    }

    void fill(String[] data, String dataset, Path[] paths, long[] lens){
        int size = data.length;

        String hdfsDefaultName = conf.get("fs.default.name");
        String workingDir = queryConf.getWorkingDir();

        for(int i = 0, j = 0; i < size; i += 2, j++){
            int id = Integer.parseInt(data[i]);
            long length = Long.parseLong(data[i + 1]);

            paths[j] = new Path(hdfsDefaultName + workingDir + "/" + dataset + "/data/" + id);
            lens[j] = length;
        }
    }

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {

        System.out.println("INFO: in getSplits");

        List<InputSplit> finalSplits = new ArrayList<InputSplit>();
        conf = job.getConfiguration();
        queryConf = new SparkQueryConf(conf);

        dataset1 = conf.get("DATASET1");
        dataset2 = conf.get("DATASET2");

        dataset1_query = new Query(conf.get("DATASET1_QUERY"));
        dataset2_query = new Query(conf.get("DATASET2_QUERY"));

        String dataset_info = conf.get("DATASETINFO");

        if(dataset_info.length() == 0){
            return finalSplits;
        }

        // iter_type, id1:len1:id2:len2:... , id1:len1:id2:len2:... ;...

        PartitionIterator iter1, iter2 = new PostFilterIterator(dataset2_query);

        String[] splits = dataset_info.split(";");
        for(int i = 0 ;i < splits.length; i ++) {
            String[] subsplits = splits[i].split(",");
            int iter_type = Integer.parseInt(subsplits[0]);

            if (iter_type == 1) {
                iter1 = new PostFilterIterator(dataset1_query);

            } else {
                iter1 = new RepartitionIterator(dataset1_query);
                ((RepartitionIterator) iter1).setZookeeper(queryConf.getZookeeperHosts());
            }

            String[] data1 = subsplits[1].split(":");
            String[] data2 = subsplits[2].split(":");

            int size1 = data1.length / 2, size2 = data2.length / 2;

            Path[] paths1 = new Path[size1];
            long[] lens1 = new long[size1];

            Path[] paths2 = new Path[size2];
            long[] lens2 = new long[size2];

            fill(data1, dataset1, paths1, lens1);
            fill(data2, dataset2, paths2, lens2);

            SparkJoinFileSplit thissplit = formSplits(paths1, lens1, paths2, lens2, iter1, iter2);
            finalSplits.add(thissplit);
        }

        job.getConfiguration().setLong(NUM_INPUT_FILES, finalSplits.size());
        LOG.debug("Total # of splits: " + finalSplits.size());
        System.out.println("done with getting splits");

        return finalSplits;
    }

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(
            InputSplit arg0, TaskAttemptContext arg1) throws IOException,
            InterruptedException {
        return new SparkJoinRecordReader();
    }
}