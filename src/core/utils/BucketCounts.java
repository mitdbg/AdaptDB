package core.utils;

import java.io.OutputStream;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class BucketCounts {

	private String countsFile;
	private FileSystem fs;
	
	Map<Integer,Integer> bucketCountMap;	
	List<String> newLines;
	
	public final int retryIntervalMs = 1000;
	public final int maxRetryCount = 20;
	

	public BucketCounts(FileSystem fs, String countsFile){
		this.countsFile = countsFile;
		this.fs = fs;
		newLines = Lists.newArrayList();
		load();
	}

	private void load(){
		bucketCountMap = Maps.newHashMap();
		List<String> lines = HDFSUtils.readHDFSLines(fs, countsFile);
		for(String line: lines){
			String[] tokens = line.split(",");
			update(Integer.parseInt(tokens[0]), Integer.parseInt(tokens[1]));
		}
	}
	
	private void update(Integer bucket, Integer countChange){
		Integer currentCount = bucketCountMap.get(bucket);
		if(currentCount==null){
			if(countChange > 0)
				bucketCountMap.put(bucket, countChange);
		}
		else{
			if(countChange > 0)
				bucketCountMap.put(bucket, currentCount + countChange);
			else
				bucketCountMap.remove(bucket);
		}
	}
	
	public int getBucketCount(int bucketId){
		if(bucketCountMap.containsKey(bucketId))
			return bucketCountMap.get(bucketId);
		else 
			return 0;
	}
	
	public void addToBucketCount(int bucketId, int count){
		update((Integer)bucketId, (Integer)count);
		newLines.add(bucketId+","+count);	// add the new value
	}
	
	public void setToBucketCount(int bucketId, int count){
		if(bucketCountMap.containsKey(bucketId))
			newLines.add(bucketId + ",-" + bucketCountMap.get(bucketId));	// remove the current value
		bucketCountMap.put(bucketId, count);
		newLines.add(bucketId+","+count);	// add the new value
	}

	public void removeBucketCount(int bucketId){
		if(bucketCountMap.containsKey(bucketId)){
			newLines.add(bucketId + ",-" + bucketCountMap.get(bucketId));	// remove the current value
			bucketCountMap.remove((Integer)bucketId);
		}
	}

	public void compact(){
		Map<Integer,Integer> compactedBuckedCount = Maps.newHashMap();
		for(String line: newLines){
			String[] tokens = line.split(",");
			Integer bucket = Integer.parseInt(tokens[0]);
			Integer countChange = Integer.parseInt(tokens[1]);
			
			Integer currentCount = compactedBuckedCount.get(bucket);
			if(currentCount==null)
				compactedBuckedCount.put(bucket, countChange);
			else
				compactedBuckedCount.put(bucket, currentCount + countChange);
		}
		List<String> compactedLines = Lists.newArrayList();
		for(Integer partition: compactedBuckedCount.keySet())
			compactedLines.add(partition+","+compactedBuckedCount.get(partition));
		this.newLines = compactedLines;
	}
	
	public void close(){
		compact();
		OutputStream os = HDFSUtils.getOutputStreamWithRetry(fs, countsFile, retryIntervalMs, maxRetryCount);
		IOUtils.writeOutputStream(os, newLines);
		IOUtils.closeOutputStream(os);
	}
}
