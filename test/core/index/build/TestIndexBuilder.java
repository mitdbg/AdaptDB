package core.index.build;

import java.io.File;

import core.utils.ConfUtils;
import core.utils.CuratorUtils;
import core.utils.HDFSUtils;
import junit.framework.TestCase;
import core.index.Settings;
import core.index.kdtree.KDMedianTree;
import core.index.key.CartilageIndexKey;
import core.index.key.CartilageIndexKeyMT;
import core.index.robusttree.RobustTreeHs;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.fs.FileSystem;

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
	ConfUtils cfg;
	
	@Override
	public void setUp(){
		inputFilename = Settings.pathToDataset + "lineitem.tbl";
		partitionBufferSize = 5*1024*1024;
		numPartitions = 16;

		propertiesFile = Settings.cartilageConf;
		cfg = new ConfUtils(propertiesFile);
		hdfsPartitionDir = cfg.getHDFS_WORKING_DIR();
		
		key = new CartilageIndexKey('|');
		//key = new SinglePassIndexKey('|');
		builder = new IndexBuilder();

		attributes = 16;
		replication = 3;
	}

	private PartitionWriter getLocalWriter(String partitionDir){
		return new BufferedPartitionWriter(partitionDir, partitionBufferSize, numPartitions);
	}

	private PartitionWriter getHDFSWriter(String partitionDir, short replication){
		return new HDFSPartitionWriter(partitionDir, partitionBufferSize, numPartitions, replication, propertiesFile);
	}

    public void testBuildKDMedianTreeLocal(){
        File f = new File(inputFilename);
        Runtime runtime = Runtime.getRuntime();
        double samplingRate = runtime.freeMemory() / (2.0 * f.length());
        System.out.println("Sampling rate: "+samplingRate);
        builder.build(
                new KDMedianTree(samplingRate),
                key,
                inputFilename,
                getLocalWriter(localPartitionDir)
        );
    }

	public void testBuildKDMedianTreeBlockSamplingOnly(int scaleFactor) {
		int bucketSize = 64; // 64 mb
		int numBuckets = (scaleFactor * 759) / bucketSize + 1;
		System.out.println("Num buckets: "+numBuckets);
		builder.buildWithBlockSamplingDir(0.0002,
				numBuckets,
				new KDMedianTree(1),
				key,
				Settings.pathToDataset + scaleFactor + "/");
	}

	public void testBuildRobustTree(){
		builder.build(new RobustTreeHs(0.01),
						key,
						inputFilename,
						getHDFSWriter(hdfsPartitionDir, (short)replication));
	}

	public void testBuildRobustTreeBlockSampling() {
		ConfUtils cfg = new ConfUtils(Settings.cartilageConf);
		CuratorFramework client = CuratorUtils.createAndStartClient(cfg.getZOOKEEPER_HOSTS());
		CuratorUtils.deleteAll(client, "/", "partition-");
		client.close();
		builder.buildWithBlockSampling(0.0002,
				new RobustTreeHs(1),
				key,
				inputFilename,
				getHDFSWriter(hdfsPartitionDir, (short) replication));
	}

	public void testBuildRobustTreeBlockSamplingOnly(int scaleFactor) {
		int bucketSize = 64; // 64 mb
		int numBuckets = (scaleFactor * 759) / bucketSize + 1;
		builder.buildWithBlockSamplingDir(0.0002,
				numBuckets,
				new RobustTreeHs(1),
				key,
				Settings.pathToDataset + scaleFactor + "/");
	}

	public void testSparkPartitioning() {
		builder.buildWithSpark(0.01,
				new RobustTreeHs(1),
				key,
				inputFilename,
				getHDFSWriter(hdfsPartitionDir, (short) replication),
				Settings.cartilageConf,
				cfg.getHDFS_WORKING_DIR());
	}

	public void testBuildRobustTreeDistributed(String partitionsId){
		ConfUtils conf = new ConfUtils(propertiesFile);
		FileSystem fs = HDFSUtils.getFS(conf.getHADOOP_HOME() + "/etc/hadoop/core-site.xml");
		byte[] indexBytes = HDFSUtils.readFile(fs, hdfsPartitionDir + "/index");
		RobustTreeHs index = new RobustTreeHs(1);
		index.unmarshall(indexBytes);
		builder.buildDistributedFromIndex(index,
				key,
				Settings.pathToDataset,
				getHDFSWriter(hdfsPartitionDir + "/partitions" + partitionsId, (short) replication));
	}

	public void testBuildRobustTreeReplicated(int scaleFactor, int numReplicas){
		int bucketSize = 64; // 64 mb
		int numBuckets = (scaleFactor * 759) / bucketSize + 1;
		ConfUtils cfg = new ConfUtils(Settings.cartilageConf);
		CuratorFramework client = CuratorUtils.createAndStartClient(cfg.getZOOKEEPER_HOSTS());
		CuratorUtils.deleteAll(client, "/", "partition-");
		client.close();
		builder.build(0.0002,
				numBuckets,
				new RobustTreeHs(1),
				key,
				Settings.pathToDataset,
				getHDFSWriter(hdfsPartitionDir, (short) replication),
				attributes,
				numReplicas
		);
	}

	public static void main(String[] args){
		System.out.println("IMBA!");
		TestIndexBuilder t = new TestIndexBuilder();
		t.setUp();
		t.testBuildRobustTree();
		//t.testBuildRobustTreeDistributed(args[args.length-1]);
//		int scaleFactor = Integer.parseInt(args[args.length - 1]);
		//t.testBuildKDMedianTreeBlockSamplingOnly(scaleFactor);
		//t.testBuildRobustTreeBlockSampling();
//		t.testBuildRobustTreeReplicated(scaleFactor, 3);
	}
}
