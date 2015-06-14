package core.index.build;

import java.io.File;

import core.utils.ConfUtils;
import core.utils.HDFSUtils;
import junit.framework.TestCase;
import core.index.Settings;
import core.index.SimpleRangeTree;
import core.index.kdtree.KDMedianTree;
import core.index.key.CartilageIndexKey;
import core.index.key.CartilageIndexKeyMT;
import core.index.robusttree.RobustTreeHs;
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

	@Override
	public void setUp(){
		inputFilename = Settings.tpchPath + "lineitem.tbl";
		partitionBufferSize = 5*1024*1024;
		numPartitions = 16;

		localPartitionDir = Settings.localPartitionDir;
		hdfsPartitionDir = Settings.hdfsPartitionDir;
		propertiesFile = Settings.cartilageConf;

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

	public void testReader(){
		long startTime = System.nanoTime();
		InputReader r = new InputReader(new SimpleRangeTree(numPartitions), key);
		r.scan(inputFilename);
		double time1 = (System.nanoTime()-startTime)/1E9;
		System.out.println("Time = "+time1+" sec");
	}

	public void testReaderMultiThreaded(){
		long startTime = System.nanoTime();
		int numThreads = 1;
		CartilageIndexKey[] keys = new CartilageIndexKey[numThreads];
		for(int i=0; i<keys.length; i++)
			keys[i] = new CartilageIndexKeyMT('|');

		InputReaderMT r = new InputReaderMT(new SimpleRangeTree(numPartitions), keys);
		r.scan(inputFilename, numThreads);
		double time1 = (System.nanoTime()-startTime)/1E9;
		System.out.println("Time = "+time1+" sec");
	}

	public void testBuildSimpleRangeTreeLocal(){
		builder.build(new SimpleRangeTree(numPartitions),
				key,
				inputFilename,
				getLocalWriter(localPartitionDir)
		);
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
				Settings.tpchPath + scaleFactor + "/");
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
						getHDFSWriter(hdfsPartitionDir, (short) replication)
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

	public void testBuildRobustTree(){
		builder.build(new RobustTreeHs(0.01),
						key,
						inputFilename,
						getHDFSWriter(hdfsPartitionDir, (short)replication));
	}

	public void testBuildRobustTreeBlockSampling() {
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
				Settings.tpchPath+scaleFactor+"/");
	}

	public void testSparkPartitioning() {
		builder.buildWithSpark(0.01,
				new RobustTreeHs(1),
				key,
				inputFilename,
				getHDFSWriter(hdfsPartitionDir, (short) replication),
				Settings.cartilageConf,
				Settings.hdfsPartitionDir);
	}

	public void testBuildRobustTreeDistributed(String partitionsId){
		ConfUtils conf = new ConfUtils(propertiesFile);
		FileSystem fs = HDFSUtils.getFS(conf.getHADOOP_HOME() + "/etc/hadoop/core-site.xml");
		byte[] indexBytes = HDFSUtils.readFile(fs, hdfsPartitionDir + "/index");
		RobustTreeHs index = new RobustTreeHs(1);
		index.unmarshall(indexBytes);
		builder.buildDistributedFromIndex(index,
				key,
				Settings.tpchPath,
				getHDFSWriter(hdfsPartitionDir+"/partitions"+partitionsId, (short) replication));
	}

	public static void main(String[] args){
		System.out.println("IMBA!");
		TestIndexBuilder t = new TestIndexBuilder();
		t.setUp();
		//t.testBuildRobustTreeDistributed(args[args.length-1]);
		//int scaleFactor = Integer.parseInt(args[args.length - 1]);
		//t.testBuildKDMedianTreeBlockSamplingOnly(scaleFactor);
		t.testBuildRobustTreeBlockSampling();
	}
}
