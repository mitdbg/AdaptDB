package perf.benchmark;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import core.index.MDIndex;
import core.index.key.CartilageIndexKeySet;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.fs.FileSystem;

import core.index.build.BufferedPartitionWriter;
import core.index.build.HDFSPartitionWriter;
import core.index.build.IndexBuilder;
import core.index.build.PartitionWriter;
import core.index.kdtree.KDMedianTree;
import core.index.key.CartilageIndexKey;
import core.index.robusttree.RobustTreeHs;
import core.utils.ConfUtils;
import core.utils.CuratorUtils;
import core.utils.HDFSUtils;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class RunIndexBuilder {
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
	
	public void setUp(){
		inputFilename = BenchmarkSettings.pathToDataset + "lineitem.tbl";
		partitionBufferSize = 5*1024*1024;
		numPartitions = 16;

		propertiesFile = BenchmarkSettings.cartilageConf;
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
				BenchmarkSettings.pathToDataset + scaleFactor + "/");
	}

	public void testBuildRobustTree(){
		builder.build(new RobustTreeHs(0.01),
				key,
				inputFilename,
				getHDFSWriter(hdfsPartitionDir, (short) replication));
	}

	public void testBuildRobustTreeBlockSampling() {
		ConfUtils cfg = new ConfUtils(BenchmarkSettings.cartilageConf);
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
				BenchmarkSettings.pathToDataset + scaleFactor + "/");
	}

	public void testWritePartitionsFromIndex(String partitionsId){
		ConfUtils conf = new ConfUtils(propertiesFile);
		FileSystem fs = HDFSUtils.getFS(conf.getHADOOP_HOME() + "/etc/hadoop/core-site.xml");
		byte[] indexBytes = HDFSUtils.readFile(fs, hdfsPartitionDir + "/index");
		RobustTreeHs index = new RobustTreeHs(1);
		index.unmarshall(indexBytes);
		builder.buildDistributedFromIndex(index,
				key,
				BenchmarkSettings.pathToDataset,
				getHDFSWriter(hdfsPartitionDir + "/partitions" + partitionsId, (short) replication));
	}

	public void testWritePartitionsFromIndexReplicated(String partitionsId){
		ConfUtils conf = new ConfUtils(propertiesFile);
		FileSystem fs = HDFSUtils.getFS(conf.getHADOOP_HOME() + "/etc/hadoop/core-site.xml");
		MDIndex[] indexes = new MDIndex[replication];
		CartilageIndexKey[] keys = new CartilageIndexKey[replication];
		PartitionWriter[] writers = new PartitionWriter[replication];
		long start = System.nanoTime();
		for (int i = 0; i < replication; i++) {
			byte[] indexBytes = HDFSUtils.readFile(fs, hdfsPartitionDir + "/" + i + "/index");
			RobustTreeHs index = new RobustTreeHs(1);
			index.unmarshall(indexBytes);
			indexes[i] = index;

			String keyString = new String(HDFSUtils.readFile(fs, hdfsPartitionDir + "/" + i + "/info"));
			keys[i] = new CartilageIndexKey(keyString);

			writers[i] = getHDFSWriter(hdfsPartitionDir + "/" + i + "/partitions"+ partitionsId, (short)1);
		}
		builder.buildDistributedReplicasFromIndex(indexes,
				keys,
				BenchmarkSettings.pathToDataset,
				"orders.tbl.",
				writers);
		System.out.println("PARTITIONING TOTAL for replica: " + (System.nanoTime() - start) / 1E9);
	}

	public void testBuildReplicatedRobustTree(int scaleFactor, int numReplicas){
		int bucketSize = 64; // 64 mb
		int numBuckets = (scaleFactor * 759) / bucketSize + 1;
		ConfUtils cfg = new ConfUtils(BenchmarkSettings.cartilageConf);
		CuratorFramework client = CuratorUtils.createAndStartClient(cfg.getZOOKEEPER_HOSTS());
		CuratorUtils.deleteAll(client, "/", "partition-");
		client.close();
		builder.build(0.0002,
				numBuckets,
				new RobustTreeHs(1),
				key,
				BenchmarkSettings.pathToDataset,
				getHDFSWriter(hdfsPartitionDir, (short) replication),
				attributes,
				numReplicas
		);
	}

	public void testBuildRobustTreeFromSamples(int tpchSize) {
		int bucketSize = 64; // 64 mb
		int numBuckets = tpchSize / bucketSize + 1;

		ConfUtils conf = new ConfUtils(propertiesFile);
		FileSystem fs = HDFSUtils.getFS(conf.getHADOOP_HOME() + "/etc/hadoop/core-site.xml");
		long startTime = System.nanoTime();

		// read all the sample files and put them into the sample key set
		CartilageIndexKeySet sample = new CartilageIndexKeySet();
		try {
			RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path(conf.getHDFS_WORKING_DIR()), false);
			while (files.hasNext()) {
				String path = files.next().getPath().toString();
				byte[] bytes = HDFSUtils.readFile(fs, path);
				sample.unmarshall(bytes);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		RobustTreeHs index = new RobustTreeHs(1);
		System.out.println("BUILD: scan sample time = " + ((System.nanoTime() - startTime) / 1E9));

		builder.buildWithSample(sample, numBuckets, index, getHDFSWriter(hdfsPartitionDir, (short) replication));
	}

	public void testBuildReplicatedRobustTreeFromSamples(int tpchSize, int numReplicas) {
		ConfUtils cfg = new ConfUtils(BenchmarkSettings.cartilageConf);
		FileSystem fs = HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME());
		List<String> paths = new ArrayList<String>();
		try {
			RemoteIterator<LocatedFileStatus> itr = fs.listFiles(new Path(cfg.getHDFS_WORKING_DIR()), false);
			while (itr.hasNext()) {
				paths.add(itr.next().getPath().getName());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		String[] samples = new String[paths.size()];
		for (int i = 0; i < samples.length; i++) {
			samples[i] =  new String(HDFSUtils.readFile(fs, cfg.getHDFS_WORKING_DIR()+"/"+paths.get(i)));
		}
		//String sample = new String(HDFSUtils.readFile(fs, Settings.hdfsPartitionDir+"/sample"));
		int bucketSize = 64; // 64 mb
		int numBuckets = tpchSize / bucketSize + 1;
		builder.buildReplicatedWithSample(
				samples,
				numBuckets,
				new RobustTreeHs(1),
				key,
				getHDFSWriter(hdfsPartitionDir, (short) replication),
				attributes,
				numReplicas
		);
	}

	public static void main(String[] args){
		// TODO(anil): Create a single index builder.
		RunIndexBuilder t = new RunIndexBuilder();
		t.setUp();
		t.testBuildRobustTree();
	}
}
