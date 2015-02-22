package core.adapt.partition;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Lists;

import core.adapt.partition.merger.PartitionMerger.KWayMerge;
import core.utils.ConfUtils;
import core.utils.HDFSUtils;
import core.utils.RangeUtils;

public class TestHDFSRepartitionIterator extends TestRepartitionIterator{

	String propertiesFile;
	
	public void setUp(){
		propertiesFile = "/Users/alekh/Work/Cartilage/MDIndex/conf/cartilage.properties";
		partitionDir = "/mydir";
		attributeIdx = 0;
		r = RangeUtils.closed(3000000, 6000000);
		
		merger = new KWayMerge(0,2);
		partitionPaths = Lists.newArrayList();
		
		FileSystem hdfs = HDFSUtils.getFS(ConfUtils.create(propertiesFile, "defaultHDFSPath").getHadoopHome()+"/etc/hadoop/core-site.xml");
		try {
			for(FileStatus fileStatus: hdfs.listStatus(new Path(partitionDir))){
				if(fileStatus.isFile() && !fileStatus.getPath().getName().startsWith("."))
					partitionPaths.add(fileStatus.getPath().toUri().getPath());	// Do something with child
			}
		} catch (IOException e) {
			System.out.println("No files to repartition");
			//e.printStackTrace();
		} 
	}

	protected Partition getPartitionInstance(String path){
		return new HDFSPartition(path, propertiesFile, (short)1);
	}
	
	public void testRepartition(){
		super.testRepartition();
	}

	public void testRepartitionAll(){
		super.testRepartitionAll();
	}
	
	public void testRepartitionFraction(){
		super.testRepartitionFraction();
	}
}
