package core.access.spark;

import java.util.List;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;

import core.access.AccessMethod;
import core.access.AccessMethod.PartitionSplit;
import core.access.Predicate;
import core.access.Query.FilterQuery;
import core.access.iterator.DistributedRepartitionIterator;
import core.access.iterator.PostFilterIterator;

public class HPInput {

	protected AccessMethod am;	
	protected ArrayListMultimap<Integer,FileStatus> partitionIdFileMap;
	protected Map<Integer,Long> partitionIdSizeMap;
	
	
	public void initialize(List<FileStatus> files, AccessMethod am){
		this.am = am;
		partitionIdFileMap = ArrayListMultimap.create();
		partitionIdSizeMap = Maps.newHashMap();
		for(FileStatus file: files){
			try {
				int id = Integer.parseInt(FilenameUtils.getName(file.getPath().toString()));
				partitionIdFileMap.put(id, file);
				long currentSize = partitionIdSizeMap.containsKey(id) ? partitionIdSizeMap.get(id) : 0;
				partitionIdSizeMap.put(id, currentSize + file.getLen());
			} catch (NumberFormatException e) {
			}
		}
	}
	
	public PartitionSplit[] getFullScan(Predicate...predicates){
		return new PartitionSplit[]{ new PartitionSplit(
				Ints.toArray(partitionIdFileMap.keySet()),
				new PostFilterIterator(
						new FilterQuery(predicates, am.getKey()))
					)
		};
	}
	
	public PartitionSplit[] getRepartitionScan(Predicate...predicates){
		return new PartitionSplit[]{new PartitionSplit(
				Ints.toArray(partitionIdFileMap.keySet()),
				new DistributedRepartitionIterator(
						new FilterQuery(predicates, am.getKey()), am.getIndex().getRoot()
					)
		)};
	}
	
	
	
	public PartitionSplit[] getIndexScan(boolean justAccess, Predicate...predicates){
		return am.getPartitionSplits(
					new FilterQuery(predicates, am.getKey()), justAccess
				);
	}
	
	
	
	
	// utility methods
	
	public Path[] getPaths(int[] partitionIds){
		List<Path> splitFiles = Lists.newArrayList();
		for(int i=0;i<partitionIds.length;i++)
			for(FileStatus fs: partitionIdFileMap.get(partitionIds[i]))
				splitFiles.add(fs.getPath());
		Path[] splitFilesArr = new Path[splitFiles.size()];
		for(int i=0;i<splitFilesArr.length;i++)
			splitFilesArr[i] = splitFiles.get(i);
		
		return splitFilesArr;
	}

	public Path[] getPaths(PartitionSplit[] splits) {
		int totalLength = 0;
		for (PartitionSplit split : splits) {
			totalLength += split.getPartitions().length;
		}
		int[] partitions = new int[totalLength];
		int currentIndex = 0;
		for (PartitionSplit split : splits) {
			for (int id : split.getPartitions()) {
				partitions[currentIndex] = id;
				currentIndex++;
			}
		}
		return getPaths(partitions);
	}
	
	public long[] getLengths(int[] partitionIds){
		List<Long> lengths = Lists.newArrayList();
		for(int i=0;i<partitionIds.length;i++)
			for(FileStatus fs: partitionIdFileMap.get(partitionIds[i]))
				lengths.add(fs.getLen());
		
		long[] lengthsArr = new long[lengths.size()];
		for(int i=0;i<lengthsArr.length;i++)
			lengthsArr[i] = lengths.get(i);
		
		return lengthsArr;
	}

	public long[] getLengths(PartitionSplit[] splits) {
		int totalLength = 0;
		for (PartitionSplit split : splits) {
			totalLength += split.getPartitions().length;
		}
		int[] partitions = new int[totalLength];
		int currentIndex = 0;
		for (PartitionSplit split : splits) {
			for (int id : split.getPartitions()) {
				partitions[currentIndex] = id;
				currentIndex++;
			}
		}
		return getLengths(partitions);
	}

	public long getTotalSize(PartitionSplit[] splits) {
		long length = 0;
		for (PartitionSplit split : splits) {
			for (int id : split.getPartitions()) {
				for (FileStatus fs : partitionIdFileMap.get(id)) {
					length += fs.getLen();
				}
			}
		}
		return length;
	}

	
	public int getNumPartitions(){
		return partitionIdSizeMap.size();
	}
	
	public Map<Integer,Long> getPartitionIdSizeMap(){
		return partitionIdSizeMap;
	}
}
