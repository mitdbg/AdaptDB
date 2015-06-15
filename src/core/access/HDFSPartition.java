package core.access;

import java.io.IOException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.io.ByteStreams;

import core.index.MDIndex;
import core.utils.ConfUtils;
import core.utils.CuratorUtils;
import core.utils.HDFSUtils;

public class HDFSPartition extends Partition{

	private static final long serialVersionUID = 1L;
	
	protected FileSystem hdfs;
	protected short replication;
	
	protected FSDataInputStream in;
	protected long totalSize=0, readSize=0, returnSize=0;
	public static int MAX_READ_SIZE = 1024*1024*50;
//	CuratorFramework client;
	String zookeeperHosts;


	public HDFSPartition(String path, String propertiesFile) {
		this(path,propertiesFile, (short)3);
	}
	
	public HDFSPartition(String pathAndPartitionId, String propertiesFile, short replication) {
		super(pathAndPartitionId);
		ConfUtils conf = new ConfUtils(propertiesFile);
		String coreSitePath = conf.getHADOOP_HOME()+"/etc/hadoop/core-site.xml";
		Configuration e = new Configuration();
		e.addResource(new Path(coreSitePath));
		try {
			this.hdfs = FileSystem.get(e);
			this.replication = replication;
		} catch (IOException ex) {
			throw new RuntimeException("failed to get hdfs filesystem");
		}
		//client = CuratorUtils.createAndStartClient(conf.getZOOKEEPER_HOSTS());
	}

//	public HDFSPartition(FileSystem hdfs, String pathAndPartitionId, short replication, CuratorFramework client) {
//		super(pathAndPartitionId);
//		this.hdfs = hdfs;
//		this.replication = replication;
//		this.client = client;
//	}
//
//	public HDFSPartition(FileSystem hdfs, String pathAndPartitionId, CuratorFramework client) {
//		this(hdfs, pathAndPartitionId, (short)3, client);
//	}
	
	public HDFSPartition(FileSystem hdfs, String pathAndPartitionId, short replication, String zookeeperHosts) {
		super(pathAndPartitionId);
		this.hdfs = hdfs;
		this.replication = replication;
		this.zookeeperHosts = zookeeperHosts;
	}

	public HDFSPartition(FileSystem hdfs, String pathAndPartitionId, String zookeeperHosts) {
		this(hdfs, pathAndPartitionId, (short)3, zookeeperHosts);
	}

	
	public Partition clone() {
		String clonePath = path.replaceAll("partitions[0-9]*/$", "repartition/");	
		Partition p = new HDFSPartition(hdfs, clonePath + partitionId, zookeeperHosts);
		//p.bytes = new byte[bytes.length]; // heap space!
		p.bytes = new byte[1024];
		p.state = State.NEW;
        return p;
    }

	public FileSystem getFS() {
		return hdfs;
	}

//	public Partition createChild(int childId){
//		Partition p = new HDFSPartition(path+"_"+childId, propertiesFile, replication);
//		p.bytes = new byte[bytes.length];	// child cannot have more bytes than parent
//		//p.bytes = new byte[8*1024*1024];	// child cannot have more than 8m bytes
//		p.state = State.NEW;
//		return p;
//	}
	
	public boolean loadNext(){
		try {
			if(totalSize==0){
				Path p = new Path(path + "/" + partitionId);
				totalSize = hdfs.getFileStatus(p).getLen();
				in = hdfs.open(p);
			}
			
			if(readSize < totalSize){
				bytes = new byte[(int)Math.min(MAX_READ_SIZE, totalSize-readSize)];
				ByteStreams.readFully(in, bytes);
				readSize += bytes.length;
				return true;
			}
			else{
				in.close();
				readSize = 0;
				totalSize = 0;
				return false;
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("Failed to read file: "+path + "/" + partitionId);
		}
	}
	
	public boolean load(){
		if(path==null || path.equals(""))
			return false;
		bytes = HDFSUtils.readFile(hdfs, path + "/" + partitionId);
		return true;	// load the physical block for this partition 
	}
	
	public byte[] getNextBytes(){
		if(readSize <= returnSize){
			boolean f = loadNext();
			if(!f)
				return null;
		}		
		returnSize += bytes.length;
		return bytes;		
	}
	
	public void store(boolean append){
		CuratorFramework client = CuratorUtils.createAndStartClient(zookeeperHosts);
		InterProcessSemaphoreMutex l = CuratorUtils.acquireLock(client, "/partition-lock-" + path.hashCode()+"-"+partitionId);
		System.out.println("LOCK: acquired lock,  "+"path="+path+" , partition id="+partitionId);
		MDIndex.BucketCounts c = new MDIndex.BucketCounts(client);

		try {
			//String storePath = FilenameUtils.getFullPath(path) + ArrayUtils.join("_", lineage);
			String storePath = path + "/" + partitionId;
			if(!path.startsWith("hdfs"))
				storePath = "/" + storePath;
			//HDFSUtils.writeFile(hdfs, storePath, replication, bytes, 0, offset, append);
			Path e = new Path(storePath);
			FSDataOutputStream os;
			if(append && hdfs.exists(e)) {
				os = hdfs.append(e);
			} else {
				os = hdfs.create(new Path(storePath), replication);
			}
			os.write(bytes, 0, offset);
			os.flush();
			os.close();
			c.addToBucketCount(this.getPartitionId(), this.getRecordCount());
			recordCount = 0;
		} catch (IOException ex) {
			throw new RuntimeException(ex.getMessage());
		} finally {
			CuratorUtils.releaseLock(l);
			System.out.println("LOCK: released lock " + partitionId);
			client.close();
		}
		//HDFSUtils.writeFile(hdfs, storePath, replication, bytes, 0, offset, append);
	}
	
	public void drop(){
		CuratorFramework client = CuratorUtils.createAndStartClient(zookeeperHosts);
		MDIndex.BucketCounts c = new MDIndex.BucketCounts(client);
		//HDFSUtils.deleteFile(hdfs, path + "/" + partitionId, false);
		c.removeBucketCount(this.getPartitionId());
		client.close();
	}	
}
