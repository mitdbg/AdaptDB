package core.access.spark;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.google.common.collect.Maps;

import core.access.AccessMethod;
import core.access.Partition;
import core.access.iterator.PartitionIterator.IteratorRecord;

public class SparkInputFormat extends FileInputFormat<LongWritable, IteratorRecord>{

	private static final Log LOG = LogFactory.getLog(FileInputFormat.class);
	
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		
		List<InputSplit> splits = new ArrayList<InputSplit>();
		List<FileStatus> files = listStatus(job);

		SparkQueryConf queryConf = new SparkQueryConf(job.getConfiguration());
		AccessMethod am = new AccessMethod();
		am.init(queryConf.getDataset());
		
		Partition[] partitions = new Partition[files.size()];
		Map<Partition,FileStatus> partitionFileMap = Maps.newHashMap();
		for(int i=0;i<files.size();i++){
			partitions[i] = new Partition(FilenameUtils.getName(files.get(i).getPath().toString()));
			partitionFileMap.put(partitions[i], files.get(i));
		}
		
		Partition[][] partitionSplits = am.getPartitionSplits(partitions, queryConf.getPredicates(), queryConf.getWorkers());
		
		for(Partition[] partitionSplit: partitionSplits){
			Path[] splitFiles = new Path[partitionSplit.length];
			long[] start = new long[partitionSplit.length];
			long[] lengths = new long[partitionSplit.length]; 
			String[] locations = new String[partitionSplit.length];

			for(int i=0;i<partitionSplit.length;i++){
				Path splitFilePath = partitionFileMap.get(partitionSplit[i]).getPath();
				splitFiles[i] = splitFilePath;
				start[i] = 0;
				lengths[i] = partitionFileMap.get(partitionSplit[i]).getLen();
				FileSystem fs = splitFilePath.getFileSystem(job.getConfiguration());
				BlockLocation[] blkLocations = fs.getFileBlockLocations(partitionFileMap.get(partitionSplit[i]), 0, lengths[i]);
				int blkIndex = getBlockIndex(blkLocations, 0);				// Assumption: One file has only 1 block!
				locations[i] = blkLocations[blkIndex].getHosts()[0];		// Assumption: replication factor  = 1
			}

			CombineFileSplit thissplit = new CombineFileSplit(splitFiles, start, lengths, locations);
			splits.add(thissplit);
		}

		job.getConfiguration().setLong(NUM_INPUT_FILES, files.size());
		LOG.debug("Total # of splits: " + splits.size());

		return splits;
	}
	
		
	public RecordReader<LongWritable, IteratorRecord> createRecordReader(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
		return null;
	}

}
