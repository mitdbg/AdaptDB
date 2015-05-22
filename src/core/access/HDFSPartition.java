package core.access;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import core.utils.ConfUtils;
import core.utils.HDFSUtils;

public class HDFSPartition extends Partition{

	private static final long serialVersionUID = 1L;
	
	private FileSystem hdfs;
	private short replication;
	
	public HDFSPartition(String path, String propertiesFile) {
		this(path,propertiesFile, (short)3);
	}
	
	public HDFSPartition(String pathAndPartitionId, String propertiesFile, short replication) {
		super(pathAndPartitionId);
		String coreSitePath = (new ConfUtils(propertiesFile)).getHADOOP_HOME()+"/etc/hadoop/core-site.xml";
		Configuration e = new Configuration();
		e.addResource(new Path(coreSitePath));
		try {
			this.hdfs = FileSystem.get(e);
			this.replication = replication;
		} catch (IOException ex) {
			throw new RuntimeException("failed to get hdfs filesystem");
		}
	}
	
	public HDFSPartition(FileSystem hdfs, String pathAndPartitionId) {
		this(hdfs, pathAndPartitionId, (short)3);
	}
	
	public HDFSPartition(FileSystem hdfs, String pathAndPartitionId, short replication) {
		super(pathAndPartitionId);
		this.hdfs = hdfs;
		this.replication = replication;
	}
	
	public Partition clone() {
		Partition p = new HDFSPartition(hdfs, path+""+partitionId);
		p.bytes = new byte[bytes.length];
		p.state = State.NEW;
        return p;
    }

//	public Partition createChild(int childId){
//		Partition p = new HDFSPartition(path+"_"+childId, propertiesFile, replication);
//		p.bytes = new byte[bytes.length];	// child cannot have more bytes than parent
//		//p.bytes = new byte[8*1024*1024];	// child cannot have more than 8m bytes
//		p.state = State.NEW;
//		return p;
//	}
	
	public boolean load(){
		if(path==null || path.equals(""))
			return false;		
		bytes = HDFSUtils.readFile(hdfs, path + "/" + partitionId);
		return true;	// load the physical block for this partition 
	}
	
	public void store(boolean append){
		//String storePath = FilenameUtils.getFullPath(path) + ArrayUtils.join("_", lineage);
		String storePath = path + "/" + partitionId;		
		if(!path.startsWith("hdfs"))
			storePath = "/" + storePath;
		HDFSUtils.writeFile(hdfs, storePath, replication, bytes, 0, offset, append);
	}
	
	public void drop(){
		HDFSUtils.deleteFile(hdfs, path + "/" + partitionId, false);
	}
}
