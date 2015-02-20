package core.adapt.partition;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.FileSystem;

import core.conf.CartilageConf;
import core.utils.ArrayUtils;
import core.utils.ConfUtils;
import core.utils.HDFSUtils;

public class HDFSPartition extends Partition{

	private FileSystem hdfs;
	private short replication = 3;
	
	public HDFSPartition(String path, String propertiesFile) {
		super(path);
		CartilageConf conf = ConfUtils.create(propertiesFile, "defaultHDFSPath");		
		hdfs = HDFSUtils.getFS(conf.getHadoopHome()+"/etc/hadoop/core-site.xml");
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
}
