package core.index.build;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;

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
	
	public void build(MDIndex[] indexes, CartilageIndexKey2[] keys, String inputFilename, PartitionWriter[] writers){
		
		long startTime = System.nanoTime();
		for(MDIndex index: indexes)
			index.initBuild(bucketSize);
		ReplicatedInputReader r = new ReplicatedInputReader(indexes, keys);
		r.scan(inputFilename);
		for(MDIndex index: indexes)
			index.initProbe();
		double time1 = (double)(System.nanoTime()-startTime)/1E9;
		System.out.println("Index Build Time = "+time1+" sec");
		
		startTime = System.nanoTime();
		r.scan(inputFilename, writers);
		for(PartitionWriter writer: writers)
			writer.flush();
		double time2 = (double)(System.nanoTime()-startTime)/1E9;
		System.out.println("Index Probe Time = "+time2+" sec");
		
		System.out.println("Total time = "+(time1+time2)+" sec");
	}
	
	public void build(MDIndex index, CartilageIndexKey2 key, String inputFilename, PartitionWriter writer, int attributes, int replication){
		int attrPerReplica = attributes / replication;

		Map<Integer,List<Integer>> replicaAttrs = Maps.newHashMap();
		for(int j=0;j<attributes;j++){
			int r = j/attrPerReplica >= replication ? replication-1 : j/attrPerReplica;
			if(!replicaAttrs.containsKey(r))
				replicaAttrs.put(r, new ArrayList<Integer>());
			replicaAttrs.get(r).add(j);			
		}

		MDIndex[] indexes = new MDIndex[replication];
		CartilageIndexKey2[] keys = new CartilageIndexKey2[replication];
		PartitionWriter[] writers = new PartitionWriter[replication];
		
		for(int i=0;i<replication;i++){
			try {
				keys[i] = (CartilageIndexKey2) key.clone();
				keys[i].setKeys(Ints.toArray(replicaAttrs.get(i)));
				indexes[i] = index.clone();
				writers[i] = (PartitionWriter) writer.clone();
				writers[i].setPartitionDir(writer.getPartitionDir()+"/"+i);
				writers[i].createPartitionDir();
			} catch (CloneNotSupportedException e) {
				e.printStackTrace();
			}
		}
		
		build(indexes, keys, inputFilename, writers);
	}

}
