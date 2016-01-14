package core.adapt.spark.join;

import core.adapt.Query;
import core.adapt.iterator.PartitionIterator;
import core.adapt.iterator.PostFilterIterator;

import core.adapt.iterator.RepartitionIterator;
import core.adapt.spark.SparkInputFormat;
import core.adapt.spark.SparkInputFormat.SparkFileSplit;
import core.adapt.spark.SparkQueryConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ylu on 1/6/16.
 */

public class SparkScanInputFormat extends FileInputFormat<LongWritable, Text> implements Serializable {

    private static int split_size = 2;

    private String dataset;
    private Query query;

    private int flag; // 1 for split, 2 for data bucket id
    private Configuration conf;
    private SparkQueryConf queryConf;

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {

        System.out.println("In SparkScanInputFormat getSplits");

        List<InputSplit> finalSplits = new ArrayList<InputSplit>();

        conf = job.getConfiguration();

        queryConf = new SparkQueryConf(conf);

        flag =  Integer.parseInt(conf.get("DATASETFLAG"));

        if (flag == 1) {
            dataset = conf.get("DATASET1");
        } else {
            dataset = conf.get("DATASET2");
        }

        query = queryConf.getQuery();

        String dataset_info = conf.get("DATASETINFO");

        if(dataset_info.length() == 0){
            return finalSplits;
        }

        PartitionIterator iter1, iter2 = new PostFilterIterator(query);

        String hdfsDefaultName = conf.get("fs.default.name");
        String workingDir = queryConf.getWorkingDir();


        String[] splits = dataset_info.split(";");

        if(flag == 1){
            // handling split
            for(int i = 0 ;i < splits.length; i ++){
                String[] subsplits = splits[i].split(",");
                int iter_type = Integer.parseInt(subsplits[0]);

                if (iter_type == 1) {
                    iter1 = new PostFilterIterator(query);

                } else {
                    iter1 = new RepartitionIterator(query);
                    ((RepartitionIterator) iter1).setZookeeper(queryConf.getZookeeperHosts());
                }

                Path[] path = new Path[subsplits.length - 1];
                long[] len = new long[subsplits.length - 1];
                for(int j = 0 ;j < path.length; j ++){
                    String[] ss = subsplits[j+1].split(":");
                    int id = Integer.parseInt(ss[0]);
                    long length =  Long.parseLong(ss[1]);
                    path[j] = new Path(hdfsDefaultName + workingDir + "/" + dataset + "/data/" + id);
                    len[j] = length;

                    //System.out.println("<<< " + path[j].toString());
                }

                SparkFileSplit split = new SparkFileSplit(path, len, iter1);
                finalSplits.add(split);
            }
        } else {
            // handling bucketID

            for(int i = 0 ;i < splits.length; i += split_size){
                int size = Math.min(split_size, splits.length - i);
                Path[] path = new Path[size];
                long[] len = new long[size];
                for(int j = 0 ;j < size; j ++ ){
                    String[] subsplits = splits[i + j].split(":");
                    int id = Integer.parseInt(subsplits[0]);
                    long length = Long.parseLong(subsplits[1]);
                    path[j] = new Path(hdfsDefaultName + workingDir + "/" + dataset + "/data/" + id);
                    len[j] = length;

                    //System.out.println(">>> " + path[j].toString());
                }
                SparkFileSplit split = new SparkFileSplit(path, len, iter2);
                finalSplits.add(split);
            }
        }

        job.getConfiguration().setLong(NUM_INPUT_FILES, finalSplits.size());
        System.out.println("done with getting splits");
        return finalSplits;
    }

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new SparkScanRecordReader();
    }
}
