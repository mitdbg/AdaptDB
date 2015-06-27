package core.access.iterator;

import java.io.File;
import java.util.List;

import junit.framework.TestCase;

import com.google.common.collect.Lists;

import core.access.Partition;
import core.access.Predicate;
import core.access.Predicate.PREDTYPE;
import core.access.Query.FilterQuery;
import core.index.Settings;
import core.index.robusttree.RNode;
import core.utils.TypeUtils.*;

public class TestRepartitionIterator extends TestCase{

	protected String partitionDir;
	protected List<String> partitionPaths;

	protected FilterQuery query;
	protected RNode newIndexTree;


	@Override
	public void setUp(){
		partitionDir = Settings.localPartitionDir;
		int attributeIdx = 0;
		//Range r = RangeUtils.closed(3000000, 6000000);
		Predicate p1 = new Predicate(attributeIdx, TYPE.INT, 3000000, PREDTYPE.GEQ);
		Predicate p2 = new Predicate(attributeIdx, TYPE.INT, 6000000, PREDTYPE.LEQ);
		query = new FilterQuery(new Predicate[]{p1,p2});

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
		PartitionIterator itr =  new RepartitionIterator(query, newIndexTree);
		itr.setPartition(partition);

		int recCount = 0;
		while(itr.hasNext()){
			itr.next();
			recCount++;
		}
		System.out.println("Number of records = "+recCount);


	}

	public void testRepartitionAll(){
		Partition partition = getPartitionInstance("");
		PartitionIterator itr =  new RepartitionIterator(query, newIndexTree);

		int recCount = 0;
		long startTime = System.nanoTime();
		for(String partitionPath: partitionPaths){
			partition.setPathAndPartitionId(partitionPath);
			partition.load();
			itr.setPartition(partition);

			while(itr.hasNext()){
				itr.next();
				recCount++;
			}
		}
		System.out.println("Number of records = "+recCount);
		double time = (System.nanoTime()-startTime)/1E9;
		System.out.println("Scan time: "+time+" secs");
	}


	public void testRepartitionFraction(){
		Partition partition = getPartitionInstance("");
		PartitionIterator itr1 =  new RepartitionIterator(query, newIndexTree);
		PostFilterIterator itr2 =  new PostFilterIterator();


		double fraction = 0.4;

		int repartitionCount = (int)(fraction*partitionPaths.size());
		int recCount = 0;
		long startTime = System.nanoTime();
		for(int i=0; i<partitionPaths.size(); i++){
			partition.setPathAndPartitionId(partitionPaths.get(i));
			partition.load();
			PartitionIterator itr;
			if(i<repartitionCount)
				itr = itr1;
			else
				itr = itr2;

			itr.setPartition(partition);

			while(itr.hasNext()){
				itr.next();
				recCount++;
			}
		}
		System.out.println("Number of records = "+recCount);
		double time = (System.nanoTime()-startTime)/1E9;
		System.out.println("Fraction: "+fraction+", Scan time: "+time+" secs");

	}
}
