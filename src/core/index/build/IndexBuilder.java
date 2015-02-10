package core.index.build;

import core.index.MDIndex;
import core.index.key.CartilageIndexKey2;

public class IndexBuilder {

	int bucketSize = 64*1024*1024;
	
	public void build(MDIndex index, CartilageIndexKey2 key, String inputFilename, PartitionWriter writer){
		
		long startTime = System.nanoTime();
		index.initBuild(bucketSize);
		InputReader r = new InputReader(index, key);
		r.scan(inputFilename);		
		index.initProbe();
		double time1 = (double)(System.nanoTime()-startTime)/1E9;
		System.out.println("Index Build Time = "+time1+" sec");
		
		startTime = System.nanoTime();
		r.scan(inputFilename, writer);		
		writer.flush();
		double time2 = (double)(System.nanoTime()-startTime)/1E9;
		System.out.println("Index Probe Time = "+time2+" sec");
		
		System.out.println("Total time = "+(time1+time2)+" sec");
	}
	
}
