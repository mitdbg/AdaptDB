package core.adapt.partition;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Lists;

import core.access.HDFSPartition;
import core.access.Partition;
import core.access.Predicate;
import core.utils.ConfUtils;
import core.utils.HDFSUtils;
import core.utils.RangeUtils;
import core.utils.RangeUtils.Range;

public class TestHDFSScanIterator extends TestScanIterator{

	String propertiesFile;

	@Override
	public void setUp(){
		propertiesFile = "/Users/alekh/Work/Cartilage/MDIndex/conf/cartilage.properties";
		partitionDir = "/mydir";
		int attributeIdx = 0;
		Range r = RangeUtils.closed(3000000, 6000000);
		predicate = new Predicate[]{new Predicate(attributeIdx, r)};

		partitionPaths = Lists.newArrayList();

		FileSystem hdfs = HDFSUtils.getFS(ConfUtils.create(propertiesFile, "defaultHDFSPath").getHadoopHome()+"/etc/hadoop/core-site.xml");
		try {
			for(FileStatus fileStatus: hdfs.listStatus(new Path(partitionDir))){
				if(fileStatus.isFile() && !fileStatus.getPath().getName().startsWith("."))
					partitionPaths.add(fileStatus.getPath().toUri().getPath());	// Do something with child
			}
		} catch (IOException e) {
			System.out.println("No files to repartition");
		}
	}

	@Override
	protected Partition getPartitionInstance(String path){
		return new HDFSPartition(path, propertiesFile, (short)1);
	}

	@Override
	public void testScan(){
		super.testScan();
	}

	@Override
	public void testScanAll(){
		super.testScanAll();
	}
}
