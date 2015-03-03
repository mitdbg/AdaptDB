package core.adapt.partition;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Lists;

import core.adapt.partition.HDFSPartition;
import core.adapt.partition.Partition;
import core.adapt.partition.TestRepartitionIterator;
import core.adapt.partition.merger.PartitionMerger.KWayMerge;
import core.index.Settings;
import core.utils.ConfUtils;
import core.utils.HDFSUtils;
import core.utils.RangeUtils;

public class TestHDFSRepartitionIterator extends TestRepartitionIterator {

	String propertiesFile;

	@Override
	public void setUp(){
		propertiesFile = Settings.cartilageConf;
		partitionDir = Settings.hdfsPartitionDir;
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

	@Override
	protected Partition getPartitionInstance(String path){
		return new HDFSPartition(path, propertiesFile, (short)1);
	}

	@Override
	public void testRepartition(){
		super.testRepartition();
	}

	@Override
	public void testRepartitionAll(){
		super.testRepartitionAll();
	}

	@Override
	public void testRepartitionFraction(){
		super.testRepartitionFraction();
	}
}
