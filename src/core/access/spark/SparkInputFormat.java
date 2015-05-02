package core.access.spark;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import core.access.AccessMethod;
import core.access.AccessMethod.PartitionSplit;
import core.access.Query.FilterQuery;
import core.access.iterator.DistributedRepartitionIterator;
import core.access.iterator.PartitionIterator;
import core.access.iterator.PartitionIterator.IteratorRecord;
import core.access.iterator.RepartitionIterator;
import core.utils.ReflectionUtils;

public class SparkInputFormat extends FileInputFormat<LongWritable, IteratorRecord>{

	private static final Log LOG = LogFactory.getLog(FileInputFormat.class);

	private SparkQueryConf queryConf;


	public static class SparkFileSplit extends CombineFileSplit{
		private PartitionIterator iterator;
		public SparkFileSplit(){
		}
		public SparkFileSplit(Path[] files, long[] start, long[] lengths, String[] locations, PartitionIterator iterator) {
			super(files, start, lengths, locations);
			this.iterator = iterator;
		}
		public PartitionIterator getIterator(){
			return this.iterator;
		}
		@Override
		public void write(DataOutput out) throws IOException{
			super.write(out);
			out.writeBytes(iterator.getClass().getName()+"\n");
			iterator.write(out);
		}
		@Override
		public void readFields(DataInput in) throws IOException{
			super.readFields(in);
			iterator = (PartitionIterator)ReflectionUtils.getInstance(in.readLine());
			iterator.readFields(in);
		}
	}


	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {

		List<InputSplit> finalSplits = new ArrayList<InputSplit>();
		List<FileStatus> files = listStatus(job);

		queryConf = new SparkQueryConf(job.getConfiguration());
		AccessMethod am = new AccessMethod();
		am.init(queryConf);
		// am.init(queryConf.getDataset(), queryConf.getHadoopHome());

		Map<Integer,FileStatus> partitionIdFileMap = Maps.newHashMap();
		for(FileStatus file: files)
			try{
				partitionIdFileMap.put(Integer.parseInt(FilenameUtils.getName(file.getPath().toString())), file);
			} catch (NumberFormatException e){
			}

		PartitionSplit[] splits = am.getPartitionSplits(new FilterQuery(queryConf.getPredicates()), queryConf.getWorkers(), false);
		splits = resizeSplits(splits, queryConf.getMaxSplitSize());		
		
		for(PartitionSplit split: splits){
			int[] partitionIds = split.getPartitions();
			Path[] splitFiles = new Path[partitionIds.length];
			long[] start = new long[partitionIds.length];
			long[] lengths = new long[partitionIds.length];
			String[] locations = new String[partitionIds.length];

			for(int i=0;i<partitionIds.length;i++){
				Path splitFilePath = partitionIdFileMap.get(partitionIds[i]).getPath();
				splitFiles[i] = splitFilePath;
				start[i] = 0;
				lengths[i] = partitionIdFileMap.get(partitionIds[i]).getLen();
				FileSystem fs = splitFilePath.getFileSystem(job.getConfiguration());
				BlockLocation[] blkLocations = fs.getFileBlockLocations(partitionIdFileMap.get(partitionIds[i]), 0, lengths[i]);
				int blkIndex = getBlockIndex(blkLocations, 0);				// Assumption: One file has only 1 block!
				locations[i] = blkLocations[blkIndex].getHosts()[0];		// Assumption: replication factor  = 1
			}

			PartitionIterator itr = split.getIterator();
			if(itr instanceof RepartitionIterator || itr instanceof DistributedRepartitionIterator)		// hack to set the zookeeper hosts
				((RepartitionIterator)itr).setZookeeper(queryConf.getZookeeperHosts());
			SparkFileSplit thissplit = new SparkFileSplit(splitFiles, start, lengths, locations, itr);
			finalSplits.add(thissplit);
		}

		job.getConfiguration().setLong(NUM_INPUT_FILES, files.size());
		LOG.debug("Total # of splits: " + finalSplits.size());

		return finalSplits;
	}

	/**
	 * The goal of this method is to check the size of each and break large splits into
	 * multiple smaller splits.
	 *
	 * The maximum split size is read from the configuration and it depends on the size of each machine.
	 *
	 * @param initialSplits
	 * @return
	 */
	public PartitionSplit[] resizeSplits(PartitionSplit[] initialSplits, int maxSplitSize){

		List<PartitionSplit> resizedSplits = Lists.newArrayList();

		for(PartitionSplit split: initialSplits){
			int[] partitions = split.getPartitions();
			if(partitions.length > maxSplitSize){
				// need to split the partition into smaller ones
				for(int i=0;i<partitions.length;i+=maxSplitSize){
					int to = i + maxSplitSize > partitions.length ? partitions.length : i + maxSplitSize;
					int[] subPartitions = Arrays.copyOfRange(partitions, i, to);

					PartitionIterator itr = split.getIterator();
					if(itr instanceof RepartitionIterator)
						itr = ((RepartitionIterator)itr).createDistributedIterator();
						//itr = new DistributedRepartitionIterator((RepartitionIterator)itr, queryConf.getZookeeperHosts());

					resizedSplits.add(new PartitionSplit(subPartitions, itr));
				}
			}
			else{
				resizedSplits.add(split);
			}
		}

		return (PartitionSplit[])resizedSplits.toArray();
	}



	@Override
	public RecordReader<LongWritable, IteratorRecord> createRecordReader(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
		return new SparkRecordReader();
	}

}
