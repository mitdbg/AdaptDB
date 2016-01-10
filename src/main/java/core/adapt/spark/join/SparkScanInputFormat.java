package core.adapt.spark.join;

import core.adapt.Query;
import core.adapt.iterator.PartitionIterator;
import core.adapt.iterator.PostFilterIterator;

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

    private static int split_size = 4;

    private String dataset;
    private Query query;

    private Configuration conf;
    private SparkQueryConf queryConf;

    private Path[] dataset_path;
    private long[] dataset_len;

    private void init_input(String input_info){
        // 1:123;2:234
        String[] splits = input_info.split(";");

        dataset_path = new Path[splits.length];
        dataset_len = new long[splits.length];

        String hdfsDefaultName = conf.get("fs.default.name");
        String workingDir = queryConf.getWorkingDir();

        for(int i = 0 ;i < splits.length; i ++){
            String[] subsplits = splits[i].split(":");
            int id = Integer.parseInt(subsplits[0]);
            long len =  Integer.parseInt(subsplits[1]);
            String file =  hdfsDefaultName + workingDir + "/" + dataset + "/data/" + id;
            dataset_path[i] = new Path(file);
            dataset_len[i] = len;
        }
    }

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {

        System.out.println("In SparkScanInputFormat getSplits");

        List<InputSplit> finalSplits = new ArrayList<InputSplit>();

        conf = job.getConfiguration();

        queryConf = new SparkQueryConf(conf);

        dataset = conf.get("DATASET");

        query = queryConf.getQuery();

        String dataset_info = conf.get("DATASETINFO");

        if(dataset_info.length() == 0){
            return finalSplits;
        }

        init_input(dataset_info);

        PartitionIterator iter = new PostFilterIterator(query);

        for(int i = 0 ;i < dataset_path.length; i += split_size){
            int size = Math.min(split_size, dataset_path.length - i);
            Path[] path = new Path[size];
            long[] len = new long[size];
            for(int j = 0 ;j < size; j ++ ){
                path[j] = dataset_path[i + j];
                len[j] = dataset_len[i + j];
            }
            SparkFileSplit split = new SparkFileSplit(path, len, iter);
            finalSplits.add(split);
        }

        job.getConfiguration().setLong(NUM_INPUT_FILES, dataset_path.length);
        System.out.println("done with getting splits");
        return finalSplits;
    }

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new SparkScanRecordReader();
    }
}
