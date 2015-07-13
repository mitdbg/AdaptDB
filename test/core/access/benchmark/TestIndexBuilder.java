package core.access.benchmark;

import junit.framework.TestCase;
import core.index.Settings;
import core.index.build.HDFSPartitionWriter;
import core.index.build.IndexBuilder;
import core.index.build.PartitionWriter;
import core.index.key.CartilageIndexKey;
import core.index.robusttree.RobustTreeHs;

public class TestIndexBuilder extends TestCase {
	String inputFilename;
	CartilageIndexKey key;
	IndexBuilder builder;

	int numPartitions;
	int partitionBufferSize;

	String localPartitionDir;
	String hdfsPartitionDir;

	String propertiesFile;
	int attributes;
	int replication;

	@Override
	public void setUp(){
		inputFilename = Settings.pathToDataset + "lineitem.tbl";
		partitionBufferSize = 5*1024*1024;
		numPartitions = 1024;

		localPartitionDir = Settings.localPartitionDir;
		hdfsPartitionDir = Settings.hdfsPartitionDir;
		propertiesFile = Settings.cartilageConf;

		key = new CartilageIndexKey('|');
		builder = new IndexBuilder();

		attributes = 16;
		replication = 3;
	}

	private PartitionWriter getHDFSWriter(String partitionDir, short replication){
		return new HDFSPartitionWriter(partitionDir, partitionBufferSize, numPartitions, replication, propertiesFile);
	}

	public void testBuildRobustTree() {
		builder.buildWithBlockSampling(0.01,
				new RobustTreeHs(1),
				key,
				inputFilename,
				getHDFSWriter(hdfsPartitionDir, (short)replication));
	}

	// Run as: ./hadoop jar /home/mdindex/mdindex_run.jar core.access.benchmark.TestIndexBuilder
	public static void main(String[] args) {
		System.out.println("Started BOOM! ");
		TestIndexBuilder t = new TestIndexBuilder();
		t.setUp();
		t.testBuildRobustTree();
	}
}
