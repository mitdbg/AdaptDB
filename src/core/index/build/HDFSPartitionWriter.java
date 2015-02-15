package core.index.build;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import core.conf.CartilageConf;
import core.utils.ConfUtils;
import core.utils.HDFSUtils;

public class HDFSPartitionWriter extends PartitionWriter{

	private String propertiesFile;
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
	
	public PartitionWriter clone() throws CloneNotSupportedException {
		HDFSPartitionWriter w = (HDFSPartitionWriter) super.clone();
		w.createHDFS(propertiesFile);
        return w;
	}
	
	private void createHDFS(String propertiesFile){
		this.propertiesFile = propertiesFile;
		CartilageConf conf = ConfUtils.create(propertiesFile, "defaultHDFSPath");		
		this.hdfs = HDFSUtils.getFS(conf.getHadoopHome()+"/etc/hadoop/core-site.xml");
	}
	
	public void createPartitionDir(){
		try {
			hdfs.mkdirs(new Path(partitionDir));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	protected OutputStream getOutputStream(String path){
		return HDFSUtils.getHDFSOutputStream(hdfs, path, replication, bufferPartitionSize);
	}
}
