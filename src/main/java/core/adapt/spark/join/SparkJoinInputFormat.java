package core.adapt.spark.join;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

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

    public static String SPLIT_ITERATOR = "SPLIT_ITERATOR";
    public static String BUDGET = "BUDGET";
    public static String JOINALGO = "JOINALGO";
    public static String RANDOM = "Random";
    public static String HEURISTIC = "Heuristic";
    public static String OVERLAP = "OVERLAP";
    public static String DATASETINFO1 = "DATASETINFO1";
    public static String DATASETINFO2 = "DATASETINFO2";

    private static final long serialVersionUID = 1L;
    private static final Log LOG = LogFactory.getLog(FileInputFormat.class);

    private Map<Integer, ArrayList<Integer> > overlap_chunks;
    private Map<Integer, Long > dataset1_len;
    private Map<Integer, Long > dataset2_len;

    private int budget;
    private String joinStrategy;
    private String dataset1, dataset2;
    private Query dataset1_query, dataset2_query;
    private int[] dataset1_splits;

    Configuration conf;
    SparkQueryConf queryConf;

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


    private ArrayList<Integer> getOverlappedSplits(ArrayList<Integer> split){
        ArrayList<Integer> final_split = new ArrayList<Integer>();
        HashSet<Integer> overlappedSplits = new HashSet<Integer>();
        for(int i = 0 ;i < split.size(); i ++){
            int id = split.get(i);
            if(overlap_chunks.containsKey(id)){
                overlappedSplits.addAll(overlap_chunks.get(id));
            }
        }
        final_split.addAll(overlappedSplits);
        return final_split;
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

    private void init_input(String overlap_info){
        // 1,1,3;2,2,3

        overlap_chunks = new HashMap<Integer, ArrayList<Integer> >();
        String[] splits = overlap_info.split(";");
        dataset1_splits = new int[splits.length];

        for(int i = 0 ;i < splits.length; i ++){
            String[] subsplits = splits[i].split(",");
            int u = Integer.parseInt(subsplits[0]);
            dataset1_splits[i] = u;
            overlap_chunks.put(u, new ArrayList<Integer> ());
            for(int j = 1; j < subsplits.length; j ++){
                int v =  Integer.parseInt(subsplits[j]);
                overlap_chunks.get(u).add(v);
            }
        }
    }

    private void init_filelen_helper(Map<Integer, Long > dataset_len, String len_info){
        String[] splits = len_info.split(";");
        for(int i = 0 ;i  < splits.length; i ++){
            String[] subsplits = splits[i].split(":");
            dataset_len.put(Integer.parseInt(subsplits[0]), Long.parseLong(subsplits[1]));
        }
    }

    private void init_filelen(String len1_info, String len2_info){
        // 1:1000;2:20000
        dataset1_len = new HashMap<Integer, Long >();
        dataset2_len = new HashMap<Integer, Long >();

        init_filelen_helper(dataset1_len, len1_info);
        init_filelen_helper(dataset2_len, len2_info);
    }

    private Path[] getPaths(int data_id, ArrayList<Integer> split){
        String hdfsDefaultName = conf.get("fs.default.name");
        String workingDir = queryConf.getWorkingDir();
        String input_dir;
        if(data_id == 1){
            input_dir =  hdfsDefaultName + workingDir + "/" + dataset1 + "/data";
        } else {
            input_dir =  hdfsDefaultName + workingDir + "/" + dataset2 + "/data";
        }

        Path[] splitFilesArr = new Path[split.size()];
        for (int i = 0; i < splitFilesArr.length; i++)
            splitFilesArr[i] = new Path(input_dir + "/" + split.get(i));

        return splitFilesArr;
    }
    private long[] getLengths(int data_id, ArrayList<Integer> split){
        Map<Integer, Long> dataset_len;
        if(data_id == 1){
            dataset_len =  dataset1_len;
        } else {
            dataset_len =  dataset2_len;
        }

        long[] lengthsArr = new long[split.size()];
        for (int i = 0; i < lengthsArr.length; i++)
            lengthsArr[i] = dataset_len.get(split.get(i));
        return lengthsArr;
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

        budget = Integer.parseInt(conf.get(BUDGET));

        String dataset1_info = conf.get(DATASETINFO1);
        String dataset2_info = conf.get(DATASETINFO2);

        if(dataset1_info.length() == 0){
            return finalSplits;
        }

        init_filelen(dataset1_info, dataset2_info);

        init_input(conf.get(OVERLAP));

        joinStrategy = conf.get(JOINALGO);

        //System.out.println("Barrier 4");

        JoinAlgo joinAlgo = null;

        if(joinStrategy.equals(RANDOM)){
            joinAlgo = new RandomGroup();
        } else if (joinStrategy.equals(HEURISTIC)){
            joinAlgo = new HeuristicGroup();
        }

        ArrayList<ArrayList<Integer> > splits = joinAlgo.getSplits(dataset1_splits, overlap_chunks, budget);

        PartitionIterator iter1 = new PostFilterIterator(dataset1_query);
        PartitionIterator iter2 = new PostFilterIterator(dataset2_query);

        System.out.println("INFO: in SparkJoinInputFormat");
        System.out.println(dataset1_query + " ## " + dataset2_query);

        for (ArrayList<Integer> split : splits) {

            ArrayList<Integer> dep_split = getOverlappedSplits(split);

            Path[] paths1 = getPaths(1, split);
            long[] lens1 = getLengths(1, split);
            Path[] paths2 = getPaths(2, dep_split);
            long[] lens2 = getLengths(2, dep_split);

            SparkJoinFileSplit thissplit = formSplits(paths1, lens1, paths2, lens2, iter1, iter2);
            finalSplits.add(thissplit);
        }

        job.getConfiguration().setLong(NUM_INPUT_FILES, dataset1_splits.length);
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