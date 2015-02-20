package core.index.build;

import junit.framework.TestCase;
import core.index.SimpleRangeTree;
import core.index.key.CartilageIndexKey2;

public class TestIndexBuilder extends TestCase{

	String inputFilename;
	CartilageIndexKey2 key;
	IndexBuilder builder;
	
	int numPartitions;
	int partitionBufferSize;
	
	String localPartitionDir;
	String hdfsPartitionDir;
	
	String propertiesFile;
	int attributes;
	int replication;
	
	public void setUp(){
		inputFilename = "/Users/alekh/Work/Cartilage/support/datasets/scale_1/lineitem.tbl";
		partitionBufferSize = 5*1024*1024;
		numPartitions = 100;
		
		localPartitionDir = "/Users/alekh/Work/tmp";
		hdfsPartitionDir = "/mydir";
		propertiesFile = "/Users/alekh/Work/Cartilage/MDIndex/conf/cartilage.properties";
		
		key = new CartilageIndexKey2('|');
		//key = new SinglePassIndexKey('|');
		builder = new IndexBuilder();
		
		attributes = 16;
		replication = 1;
	}
	
	private PartitionWriter getLocalWriter(String partitionDir){
		return new BufferedPartitionWriter(partitionDir, partitionBufferSize, numPartitions);
	}
	
	private PartitionWriter getHDFSWriter(String partitionDir, short replication){
		return new HDFSPartitionWriter(partitionDir, partitionBufferSize, numPartitions, replication, propertiesFile);
	}
	
	
	
	public void testBuildSimpleRangeTreeLocal(){
		builder.build(new SimpleRangeTree(numPartitions), 
						key, 
						inputFilename, 
						getLocalWriter(localPartitionDir)
					);
	}
	
	public void testBuildSimpleRangeTreeLocalReplicated(){		
		builder.build(new SimpleRangeTree(numPartitions),
						key, 
						inputFilename, 
						getLocalWriter(localPartitionDir), 
						attributes, 
						replication
					);
	}
	
	public void testBuildSimpleRangeTreeHDFS(){
		builder.build(new SimpleRangeTree(numPartitions), 
						key, 
						inputFilename, 
						getHDFSWriter(hdfsPartitionDir, (short)replication)
					);
	}
	
	public void testBuildSimpleRangeTreeHDFSReplicated(){
		builder.build(new SimpleRangeTree(numPartitions), 
						key, 
						inputFilename, 
						getHDFSWriter(hdfsPartitionDir, (short)replication),
						attributes, 
						replication
					);
	}
	
}
