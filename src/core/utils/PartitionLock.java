package core.utils;

import java.util.List;

import org.apache.hadoop.fs.FileSystem;

import com.google.common.collect.Lists;

/**
 * A class to lock a partition when writing to it.
 * 
 * @author alekh
 *
 */

public class PartitionLock {

	private List<Integer> lockedPartitions;
	private FileSystem hdfs;
	private String lockDir;
	
	private final long retryIntervalMs = 1000;
	private final int maxAttempts = 20;
	
	public PartitionLock(FileSystem fs, String lockDir){
		this.lockedPartitions = Lists.newArrayList();
		this.hdfs = fs;
		this.lockDir = lockDir;
	}
	
	public void acquire(int partitionId){
		// try create a lock file on HDFS
		boolean locked = HDFSUtils.tryCreateFile(hdfs, lockDir+"/"+partitionId);
		int attempt=1;
		while(!locked && attempt < maxAttempts){
			ThreadUtils.sleep(retryIntervalMs);
			locked = HDFSUtils.tryCreateFile(hdfs, lockDir+"/"+partitionId);
			attempt++;
		}
		if(locked)
			lockedPartitions.add(partitionId);
		else
			throw new RuntimeException("Failed to obtain a lock!");
	}
	
	public void release(int partitionId){
		// remove the lock file on HDFS
		boolean deleted = HDFSUtils.tryDelete(hdfs, lockDir+"/"+partitionId, false);
		int attempt=1;
		while(!deleted && attempt < maxAttempts){
			ThreadUtils.sleep(retryIntervalMs);
			deleted = HDFSUtils.tryDelete(hdfs, lockDir+"/"+partitionId, false);
			attempt++;
		}
		if(deleted){
			if(lockedPartitions.contains(partitionId))
				lockedPartitions.remove((Integer)partitionId);
		}
		else
			throw new RuntimeException("Failed to release a lock!");
	}
	
	public void cleanup(){
		for(Integer partitionId: lockedPartitions)
			release(partitionId);
		lockedPartitions.clear();
	}
}
