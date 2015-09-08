package core.access.iterator;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Lists;

import core.access.HDFSPartition;
import core.access.Partition;
import core.access.Predicate;
import core.access.Predicate.PREDTYPE;
import core.access.Query.FilterQuery;
import core.utils.ConfUtils;
import core.utils.HDFSUtils;
import core.utils.TypeUtils.*;
import perf.benchmark.BenchmarkSettings;

public class TestHDFSRepartitionIterator extends TestRepartitionIterator {

	@Override
	public void setUp(){
		ConfUtils cfg = new ConfUtils(BenchmarkSettings.conf);
		partitionDir = cfg.getHDFS_WORKING_DIR();
		
		int attributeIdx = 0;
		Predicate p1 = new Predicate(attributeIdx, TYPE.INT, 3000000, PREDTYPE.GEQ);
		Predicate p2 = new Predicate(attributeIdx, TYPE.INT, 6000000, PREDTYPE.LEQ);
		
		query = new FilterQuery(new Predicate[]{p1, p2});

		partitionPaths = Lists.newArrayList();
		FileSystem hdfs = HDFSUtils.getFS(cfg.getHADOOP_HOME()+"/etc/hadoop/core-site.xml");
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
		return new HDFSPartition(path, BenchmarkSettings.conf, (short)1);
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
