package core.index.build;

import junit.framework.TestCase;
import core.index.SimpleRangeTree;
import core.index.key.CartilageIndexKey2;

public class TestIndexBuilder extends TestCase{

	String inputFilename;
	CartilageIndexKey2 key;
	
	PartitionWriter writer;
	IndexBuilder builder;
	
	int numPartitions;
	
	public void setUp(){
		inputFilename = "/Users/alekh/Work/Cartilage/support/datasets/scale_1/lineitem.tbl";
		String propertiesFile = "/Users/alekh/Work/Cartilage/MDIndex/conf/cartilage.properties";
		String partitionDir = "/mydir";
		
		int partitionBufferSize = 5*1024*1024;
		numPartitions = 100;
		short replication = 1;
		
		key = new CartilageIndexKey2('|');
		writer = new HDFSPartitionWriter(partitionDir, partitionBufferSize, numPartitions, replication, propertiesFile);
		builder = new IndexBuilder();
	}
	
	public void testBuildSimpleRangeTree(){
		SimpleRangeTree index = new SimpleRangeTree(numPartitions);
		builder.build(index, key, inputFilename, writer);
	}
		
}
