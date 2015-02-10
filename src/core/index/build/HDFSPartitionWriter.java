package core.index.build;

import java.io.OutputStream;

import org.apache.hadoop.fs.FileSystem;

import core.conf.CartilageConf;
import core.utils.ConfUtils;
import core.utils.HDFSUtils;

public class HDFSPartitionWriter extends PartitionWriter{

	private short replication = 3;
	private FileSystem hdfs;
	
	public HDFSPartitionWriter(String partitionDir, int bufferPartitionSize, int maxBufferPartitions, short replication, String propertiesFile){
		super(partitionDir, bufferPartitionSize, maxBufferPartitions);
		this.replication = replication;
		createHDFS(propertiesFile);
	}
	
	public HDFSPartitionWriter(String partitionDir, int bufferPartitionSize, int maxBufferPartitions, String propertiesFile){
		super(partitionDir, bufferPartitionSize, maxBufferPartitions);
		createHDFS(propertiesFile);
	}
	
	public HDFSPartitionWriter(String partitionDir, String propertiesFile) {
		super(partitionDir);
		createHDFS(propertiesFile);
	}
	
	private void createHDFS(String propertiesFile){
		CartilageConf conf = ConfUtils.create(propertiesFile, "defaultHDFSPath");		
		this.hdfs = HDFSUtils.getFS(conf.getHadoopHome()+"/etc/hadoop/core-site.xml");
	}

	protected OutputStream getOutputStream(String path){
		return HDFSUtils.getHDFSOutputStream(hdfs, path, replication, bufferPartitionSize);
	}
}
