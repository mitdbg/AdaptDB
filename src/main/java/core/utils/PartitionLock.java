package core.utils;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

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
	// private String hadoopHome;
	private String lockDir;

	private final long retryIntervalMs = 1000;
	private final int maxAttempts = 20;

	public PartitionLock(FileSystem fs, String lockDir) {
		this.lockedPartitions = Lists.newArrayList();
		this.hdfs = fs;
		this.lockDir = lockDir;
	}

	// public PartitionLock(String hadoopHome, String lockDir){
	// this.lockedPartitions = Lists.newArrayList();
	// this.hadoopHome = hadoopHome;
	// this.lockDir = lockDir;
	// }

	private boolean createLockFile(String path) {
		try {
			// FileSystem hdfs = HDFSUtils.getFSByHadoopHome(hadoopHome);
			return hdfs.createNewFile(new Path(path));
		} catch (IOException e1) {
			System.out
					.println("failed to create lock file: " + e1.getMessage());
			return false;
		}
	}

	private boolean deleteLockFile(String path) {
		try {
			// FileSystem hdfs = HDFSUtils.getFSByHadoopHome(hadoopHome);
			return hdfs.delete(new Path(path), false);
		} catch (IOException e) {
			return false;
		}
	}

	public void acquire(int partitionId) { // should we make it synchronized?
		// try create a lock file on HDFS
		System.out.println("going to acquire");
		boolean locked = createLockFile(lockDir + "/" + partitionId);
		int attempt = 1;
		System.out.println("acuire attempt " + attempt);
		while (!locked && attempt < maxAttempts) {
			ThreadUtils.sleep(retryIntervalMs);
			locked = createLockFile(lockDir + "/" + partitionId);
			// locked = HDFSUtils.tryCreateFile(hdfs, lockDir+"/"+partitionId);
			attempt++;
			System.out.println("acuire attempt " + attempt);
		}
		if (locked)
			lockedPartitions.add(partitionId);
		else
			throw new RuntimeException(
					"Failed to obtain a lock for partition: " + partitionId);
	}

	public void release(int partitionId) {
		// remove the lock file on HDFS
		boolean deleted = deleteLockFile(lockDir + "/" + partitionId);
		int attempt = 1;
		while (!deleted && attempt < maxAttempts) {
			ThreadUtils.sleep(retryIntervalMs);
			deleted = deleteLockFile(lockDir + "/" + partitionId);
			attempt++;
		}
		if (deleted) {
			if (lockedPartitions.contains(partitionId))
				lockedPartitions.remove((Integer) partitionId);
		} else
			throw new RuntimeException(
					"Failed to release a lock for partition: " + partitionId);
	}

	public void cleanup() {
		for (Integer partitionId : lockedPartitions)
			release(partitionId);
		lockedPartitions.clear();
	}
}
