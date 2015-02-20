package core.adapt.partition;

import java.io.File;
import java.util.List;

import com.google.common.collect.Lists;

import core.adapt.partition.iterator.ScanIterator;
import core.utils.RangeUtils;
import core.utils.RangeUtils.Range;
import junit.framework.TestCase;

public class TestScanIterator extends TestCase{

	String partitionDir;
	List<String> partitionPaths;

	int attributeIdx;
	Range r;
	
	public void setUp(){
		partitionDir = "/Users/alekh/Work/tmp";
		attributeIdx = 0;
		r = RangeUtils.closed(3000000, 6000000);
		
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
		ScanIterator itr =  new ScanIterator();
		itr.setPartition(partition, attributeIdx, r);
		
		int recCount = 0;
		while(itr.hasNext()){
			itr.next();
			recCount++;			
		}
		System.out.println("Number of records = "+recCount);
	}
	
	public void testScanAll(){
		Partition partition = getPartitionInstance("");
		ScanIterator itr =  new ScanIterator();
		
		int recCount = 0;
		long startTime = System.nanoTime();
		for(String partitionPath: partitionPaths){
			partition.setPath(partitionPath);
			partition.load();
			itr.setPartition(partition, attributeIdx, r);
			
			while(itr.hasNext()){
				itr.next();
				recCount++;			
			}
		}
		System.out.println("Number of records = "+recCount);
		double time = (double)(System.nanoTime()-startTime)/1E9;
		System.out.println("Scan time: "+time+" secs");
	}
}
