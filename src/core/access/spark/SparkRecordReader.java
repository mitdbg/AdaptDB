package core.access.spark;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import core.access.HDFSPartition;
import core.access.Partition;
import core.access.iterator.PartitionIterator;
import core.access.iterator.PartitionIterator.IteratorRecord;
import core.adapt.Predicate;

public class SparkRecordReader extends RecordReader<LongWritable, IteratorRecord> {

	protected Configuration conf;

	protected CombineFileSplit combinedSplit;
	private int currentFile;

	protected Predicate[] predicates;
	protected PartitionIterator iterator;

	private LongWritable key;
	private long recordId;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		conf = context.getConfiguration();
		SparkQueryConf queryConf = new SparkQueryConf(conf);
		predicates = queryConf.getPredicates();
		iterator = new PartitionIterator();		//TODO: the iterator must be chosen based on how we want to read ..

		key = new LongWritable();
		recordId = 0;

		if(split instanceof CombineFileSplit){
			combinedSplit = (CombineFileSplit)split;
			currentFile = 0;
			initializeNext();
		}
		else
			initializeIterator(((FileSplit) split).getPath());
	}

	protected boolean initializeNext() throws IOException{
		if(currentFile >= combinedSplit.getStartOffsets().length)
			return false;
		else{
			initializeIterator(combinedSplit.getPath(currentFile));
			currentFile++;
			return true;
		}
	}

	private void initializeIterator(Path filePath) throws IOException{
		final FileSystem fs = filePath.getFileSystem(conf);
		Partition partition = new HDFSPartition(fs, filePath.toString());
		partition.load();
		iterator.setPartition(partition, predicates);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if(iterator.hasNext() || (combinedSplit!=null && initializeNext())){
			recordId++;
			return true;
		}
		else
			return false;
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		key.set(recordId);
		return key;
	}

	@Override
	public IteratorRecord getCurrentValue() throws IOException, InterruptedException {
		return iterator.next();
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return combinedSplit==null ? 0 : (float)currentFile / combinedSplit.getStartOffsets().length;
	}

	@Override
	public void close() throws IOException {
	}
}
