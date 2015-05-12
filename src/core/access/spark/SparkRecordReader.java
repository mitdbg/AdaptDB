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

import com.google.common.base.Joiner;

import core.access.HDFSPartition;
import core.access.Partition;
import core.access.iterator.PartitionIterator;
import core.access.iterator.PartitionIterator.IteratorRecord;

public class SparkRecordReader extends RecordReader<LongWritable, IteratorRecord> {

	protected Configuration conf;
	
	protected CombineFileSplit sparkSplit;
	private int currentFile;
	
	protected PartitionIterator iterator;

	private LongWritable key;
	private long recordId;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		
		System.out.println("Initializing SparkRecordReader");
		
		conf = context.getConfiguration();
		System.out.println(conf);
		sparkSplit = (CombineFileSplit)split;
		
		long splitID = Joiner.on(",").join(sparkSplit.getPaths()).hashCode();
		System.out.println("splitID = "+splitID);
		
		String iteratorString = conf.get(SparkInputFormat.SPLIT_ITERATOR + splitID);
		System.out.println("iteratorString = "+iteratorString);
		
		iterator = PartitionIterator.stringToIterator(iteratorString);
		
		currentFile = 0;
		initializeNext();
		
		key = new LongWritable();
		recordId = 0;		

	}

	protected boolean initializeNext() throws IOException{
		
		System.out.println("Initializing next partition in SparkRecordReader");
		
		if(currentFile >= sparkSplit.getStartOffsets().length)
			return false;
		else{
			Path filePath = sparkSplit.getPath(currentFile);
			final FileSystem fs = filePath.getFileSystem(conf);
			Partition partition = new HDFSPartition(fs, filePath.toString());
			partition.load();
			iterator.setPartition(partition);
			
			currentFile++;
			return true;
		}
	}

	public boolean nextKeyValue() throws IOException, InterruptedException {
		if(iterator.hasNext() || (sparkSplit!=null && initializeNext())){
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
		return (float)currentFile / sparkSplit.getStartOffsets().length;
	}

	@Override
	public void close() throws IOException {
	}
}
