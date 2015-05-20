package core.access;

import org.apache.hadoop.fs.FileSystem;

import core.utils.ConfUtils;
import core.utils.HDFSUtils;

public class HDFSPartition extends Partition{

	private FileSystem hdfs;
	private short replication;
	
	public HDFSPartition(String path, String propertiesFile) {
		this(path,propertiesFile, (short)3);
	}
	
	public HDFSPartition(String pathAndPartitionId, String propertiesFile, short replication) {
		this(HDFSUtils.getFS(ConfUtils.create(propertiesFile, "defaultHDFSPath").getHadoopHome()+"/etc/hadoop/core-site.xml"), pathAndPartitionId);
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
		HDFSUtils.writeFile(hdfs, storePath, replication, bytes, 0, offset, append);		
	}
	
	public void drop(){
		HDFSUtils.deleteFile(hdfs, path + "/" + partitionId, false);
	}
}
