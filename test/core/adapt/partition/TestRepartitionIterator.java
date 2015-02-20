package core.adapt.partition;

import java.io.File;
import java.util.List;

import junit.framework.TestCase;

import com.google.common.collect.Lists;

import core.adapt.partition.iterator.PartitionIterator;
import core.adapt.partition.iterator.RepartitionIterator;
import core.adapt.partition.iterator.ScanIterator;
import core.adapt.partition.merger.PartitionMerger;
import core.adapt.partition.merger.PartitionMerger.KWayMerge;
import core.index.key.CartilageIndexKey2;
import core.utils.RangeUtils;
import core.utils.RangeUtils.Range;

public class TestRepartitionIterator extends TestCase{

	String partitionDir;
	List<String> partitionPaths;

	int attributeIdx;
	Range r;
	
	PartitionMerger merger;
	

	public void setUp(){
		partitionDir = "/Users/alekh/Work/tmp";
		attributeIdx = 0;
		r = RangeUtils.closed(3000000, 6000000);
		
		merger = new KWayMerge(0,2);
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
	
	
	public void testRepartition(){
		Partition partition = getPartitionInstance(partitionDir+"/0");
		partition.load();
		RepartitionIterator itr =  new RepartitionIterator(merger);
		itr.setPartition(partition, attributeIdx, r);
		
		int recCount = 0;
		while(itr.hasNext()){
			itr.next();
			recCount++;			
		}
		System.out.println("Number of records = "+recCount);
		
		
	}

	public void testRepartitionAll(){
		Partition partition = getPartitionInstance("");
		RepartitionIterator itr =  new RepartitionIterator(merger);
		
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
	

	public void testRepartitionFraction(){
		Partition partition = getPartitionInstance("");
		RepartitionIterator itr1 =  new RepartitionIterator(merger);
		ScanIterator itr2 =  new ScanIterator();
		
		
		double fraction = 0.4;
		
		int repartitionCount = (int)(fraction*partitionPaths.size());
		int recCount = 0;
		long startTime = System.nanoTime();
		for(int i=0; i<partitionPaths.size(); i++){
			partition.setPath(partitionPaths.get(i));
			partition.load();
			PartitionIterator<CartilageIndexKey2> itr;
			if(i<repartitionCount)
				itr = itr1;
			else
				itr = itr2;
			
			itr.setPartition(partition, attributeIdx, r);
			
			while(itr.hasNext()){
				itr.next();
				recCount++;			
			}
		}
		System.out.println("Number of records = "+recCount);
		double time = (double)(System.nanoTime()-startTime)/1E9;
		System.out.println("Fraction: "+fraction+", Scan time: "+time+" secs");
		
	}
}
