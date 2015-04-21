package core.adapt.partition;

import java.io.File;
import java.util.List;

import junit.framework.TestCase;

import com.google.common.collect.Lists;

import core.access.Partition;
import core.access.Predicate;
import core.access.iterator.PostFilterIterator;
import core.index.Settings;
import core.utils.RangeUtils;
import core.utils.RangeUtils.Range;

public class TestScanIterator extends TestCase{

	protected String partitionDir;
	protected List<String> partitionPaths;

	protected Predicate[] predicate;

	@Override
	public void setUp(){
		partitionDir = Settings.localPartitionDir;
		int attributeIdx = 0;
		Range r = RangeUtils.closed(3000000, 6000000);
		predicate = new Predicate[]{new Predicate(attributeIdx, r)};

		partitionPaths = Lists.newArrayList();

		File dir = new File(partitionDir);
		File[] directoryListing = dir.listFiles();
		if (directoryListing != null) {
			for (File child : directoryListing)
				if(child.isFile() && !child.getName().startsWith("."))
					partitionPaths.add(child.getPath());	// Do something with child
		}
	}

	protected Partition getPartitionInstance(String path){
		return new Partition(path);
	}

	public void testScan(){
		Partition partition = getPartitionInstance(partitionDir+"/0");
		partition.load();
		PostFilterIterator itr =  new PostFilterIterator();
		itr.setPartition(partition, predicate);

		int recCount = 0;
		while(itr.hasNext()){
			itr.next();
			recCount++;
		}
		System.out.println("Number of records = "+recCount);
	}

	public void testScanAll(){
		Partition partition = getPartitionInstance("");
		PostFilterIterator itr =  new PostFilterIterator();

		int recCount = 0;
		long startTime = System.nanoTime();
		for(String partitionPath: partitionPaths){
			partition.setPath(partitionPath);
			partition.load();
			itr.setPartition(partition, predicate);

			while(itr.hasNext()){
				itr.next();
				recCount++;
			}
		}
		System.out.println("Number of records = "+recCount);
		double time = (System.nanoTime()-startTime)/1E9;
		System.out.println("Scan time: "+time+" secs");
	}
}
