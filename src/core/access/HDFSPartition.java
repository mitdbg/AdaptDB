package core.access;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.FileSystem;

import core.conf.CartilageConf;
import core.utils.ArrayUtils;
import core.utils.ConfUtils;
import core.utils.HDFSUtils;

public class HDFSPartition extends Partition{

	private FileSystem hdfs;
	private short replication;
	private String propertiesFile;
	
	public HDFSPartition(String path, String propertiesFile) {
		this(path,propertiesFile, (short)3);
	}
	
	public HDFSPartition(String path, String propertiesFile, short replication) {
		super(path);
		this.propertiesFile = propertiesFile;
		this.replication = replication;
		CartilageConf conf = ConfUtils.create(propertiesFile, "defaultHDFSPath");		
		hdfs = HDFSUtils.getFS(conf.getHadoopHome()+"/etc/hadoop/core-site.xml");
	}
	
	public HDFSPartition(FileSystem hdfs, String path) {
		this(hdfs, path, (short)3);
	}
	
	public HDFSPartition(FileSystem hdfs, String path, short replication) {
		super(path);
		this.hdfs = hdfs;
		this.replication = replication;
	}

	public Partition createChild(int childId){
		Partition p = new HDFSPartition(path+"_"+childId, propertiesFile, replication);
		p.bytes = new byte[bytes.length];	// child cannot have more bytes than parent
		//p.bytes = new byte[8*1024*1024];	// child cannot have more than 8m bytes
		p.state = State.NEW;
		return p;
	}
	
	public boolean load(){
		if(path==null || path.equals(""))
			return false;		
		bytes = HDFSUtils.readFile(hdfs, path);		
		return true;	// load the physical block for this partition 
	}
	
	public void store(boolean append){
		String storePath = FilenameUtils.getFullPath(path) + ArrayUtils.join("_", lineage);
		HDFSUtils.writeFile(hdfs, storePath, replication, bytes, 0, offset, append);		
	}
	
	public void drop(){
		HDFSUtils.deleteFile(hdfs, path, false);
	}
}
