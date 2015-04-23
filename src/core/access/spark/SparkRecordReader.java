package core.access.spark;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import core.access.HDFSPartition;
import core.access.Partition;
import core.access.iterator.PartitionIterator;
import core.access.iterator.PartitionIterator.IteratorRecord;
import core.access.spark.SparkInputFormat.SparkFileSplit;

public class SparkRecordReader extends RecordReader<LongWritable, IteratorRecord> {

	protected Configuration conf;
	
	protected SparkFileSplit sparkSplit;
	private int currentFile;
	
	protected PartitionIterator iterator;
	
	private LongWritable key;
	private long recordId;
	
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		conf = context.getConfiguration();				
		sparkSplit = (SparkFileSplit)split;
		iterator = sparkSplit.getIterator();
		
		currentFile = 0;
		initializeNext();
		
		key = new LongWritable();
		recordId = 0;		
	}
	
	protected boolean initializeNext() throws IOException{
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
	
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		key.set(recordId);
		return key;
	}

	public IteratorRecord getCurrentValue() throws IOException, InterruptedException {
		return iterator.next();
	}

	public float getProgress() throws IOException, InterruptedException {
		return (float)currentFile / sparkSplit.getStartOffsets().length;
	}
	
	public void close() throws IOException {
	}
}
