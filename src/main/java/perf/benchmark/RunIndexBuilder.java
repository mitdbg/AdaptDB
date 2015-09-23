package perf.benchmark;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import core.index.build.HDFSPartitionWriter;
import core.index.build.IndexBuilder;
import core.index.build.PartitionWriter;
import core.index.key.CartilageIndexKey;
import core.index.key.CartilageIndexKeySet;
import core.index.key.Schema;
import core.index.robusttree.RobustTreeHs;
import core.utils.ConfUtils;
import core.utils.HDFSUtils;

/**
 * Builds the index. Captures time taken by the different steps in index
 * building. TODO: Make it generalized by not depending on lineitem. TODO:
 * Compute sampling fraction instead of taking as an option.
 *
 * @author anil
 *
 */
public class RunIndexBuilder {
	CartilageIndexKey key;
	IndexBuilder builder;

	int partitionBufferSize;

	String localPartitionDir;
	String hdfsPartitionDir;

	ConfUtils cfg;

	// Directory on local file system containing the inputs.
	String inputsDir;

	// Directory in HDFS containing the samples.
	String samplesDir;

	// Specifies which method should be run.
	// See 'main' for method numbers.
	int method = -1;

	// Sampling probability.
	double samplingRate = 0;

	// Number of buckets in the index.
	int numBuckets = -1;

	// Number of replicas to use (only used when building a tree per replica).
	int numReplicas = -1;

	// Describes the schema of the input files.
	String schemaString;

	// Number of fields in the input file.
	int numFields = -1;

	public void setUp() {
		partitionBufferSize = 2 * 1024 * 1024;

		cfg = new ConfUtils(BenchmarkSettings.conf);
		hdfsPartitionDir = cfg.getHDFS_WORKING_DIR();

		key = new CartilageIndexKey('|');
		builder = new IndexBuilder();
	}

	private PartitionWriter getHDFSWriter(String partitionDir, short replication) {
		return new HDFSPartitionWriter(partitionDir, partitionBufferSize,
				replication, this.cfg);
	}

	// public void testBuildKDMedianTreeBlockSamplingOnly(int scaleFactor) {
	// int bucketSize = 64; // 64 mb
	// int numBuckets = (scaleFactor * 759) / bucketSize + 1;
	// System.out.println("Num buckets: "+numBuckets);
	// builder.buildWithBlockSamplingDir(samplingRate,
	// numBuckets,
	// new KDMedianTree(1),
	// key,
	// BenchmarkSettings.pathToDataset + scaleFactor + "/");
	// }
	//
	// public void testBuildRobustTreeBlockSampling() {
	// ConfUtils cfg = new ConfUtils(BenchmarkSettings.conf);
	// CuratorFramework client =
	// CuratorUtils.createAndStartClient(cfg.getZOOKEEPER_HOSTS());
	// CuratorUtils.deleteAll(client, "/", "partition-");
	// client.close();
	// builder.buildWithBlockSampling(samplingRate,
	// new RobustTreeHs(),
	// key,
	// inputFilename,
	// getHDFSWriter(hdfsPartitionDir, (short) replication));
	// }

	// public void testBuildRobustTreeBlockSamplingOnly(int scaleFactor) {
	// int bucketSize = 64; // 64 mb
	// int numBuckets = (scaleFactor * 759) / bucketSize + 1;
	// builder.buildWithBlockSamplingDir(samplingRate,
	// numBuckets,
	// new RobustTreeHs(),
	// key,
	// BenchmarkSettings.pathToDataset + scaleFactor + "/");
	// }
	//
	// public void testWritePartitionsFromRangeIndex(String partitionsId){
	// AttributeRangeTree index = new AttributeRangeTree(1, TypeUtils.TYPE.INT);
	// int start = 0;
	// int range = 200000000;
	// int numBuckets = 100;
	// Object[] buckets = new Integer[numBuckets+1];
	// for (int i = 0; i <= numBuckets; i++) {
	// buckets[i] = start + (range / numBuckets) * i;
	// }
	// index.setBoundaries(buckets);
	// builder.buildDistributedFromIndex(index,
	// key,
	// BenchmarkSettings.pathToDataset,
	// getHDFSWriter(hdfsPartitionDir + "/partitions" + partitionsId, (short)
	// replication));
	// }
	//
	// public void testWritePartitionsFromIndexReplicated(String partitionsId){
	// ConfUtils conf = new ConfUtils(propertiesFile);
	// FileSystem fs = HDFSUtils.getFS(conf.getHADOOP_HOME() +
	// "/etc/hadoop/core-site.xml");
	// MDIndex[] indexes = new MDIndex[replication];
	// CartilageIndexKey[] keys = new CartilageIndexKey[replication];
	// PartitionWriter[] writers = new PartitionWriter[replication];
	// long start = System.nanoTime();
	// for (int i = 0; i < replication; i++) {
	// byte[] indexBytes = HDFSUtils.readFile(fs, hdfsPartitionDir + "/" + i +
	// "/index");
	// RobustTreeHs index = new RobustTreeHs();
	// index.unmarshall(indexBytes);
	// indexes[i] = index;
	//
	// String keyString = new String(HDFSUtils.readFile(fs, hdfsPartitionDir +
	// "/" + i + "/info"));
	// keys[i] = new CartilageIndexKey(keyString);
	//
	// writers[i] = getHDFSWriter(hdfsPartitionDir + "/" + i + "/partitions"+
	// partitionsId, (short)1);
	// }
	// builder.buildDistributedReplicasFromIndex(indexes,
	// keys,
	// BenchmarkSettings.pathToDataset,
	// "orders.tbl.",
	// writers);
	// System.out.println("PARTITIONING TOTAL for replica: " +
	// (System.nanoTime() - start) / 1E9);
	// }
	//
	// public void testBuildReplicatedRobustTree(int scaleFactor, int
	// numReplicas){
	// int bucketSize = 64; // 64 mb
	// int numBuckets = (scaleFactor * 759) / bucketSize + 1;
	// CuratorFramework client =
	// CuratorUtils.createAndStartClient(cfg.getZOOKEEPER_HOSTS());
	// CuratorUtils.deleteAll(client, "/", "partition-");
	// client.close();
	// builder.build(samplingRate,
	// numBuckets,
	// new RobustTreeHs(),
	// key,
	// BenchmarkSettings.pathToDataset,
	// getHDFSWriter(hdfsPartitionDir, (short) replication),
	// attributes,
	// numReplicas
	// );
	// }

	/**
	 * Creates one sample file sample.machineId and writes it out to HDFS.
	 *
	 * @param machineId
	 *            descriptor for machine the code will run on.
	 */
	public void createSamples() {
		assert schemaString != null;
		assert numFields != -1;
		assert inputsDir != null;
		assert samplingRate != 0;
		Schema.createSchema(schemaString, numFields);

		FileSystem fs = HDFSUtils.getFS(cfg.getHADOOP_HOME()
				+ "/etc/hadoop/core-site.xml");
		builder.blockSampleInput(
				samplingRate,
				key,
				inputsDir,
				cfg.getHDFS_WORKING_DIR() + "/samples/sample."
						+ cfg.getMACHINE_ID(), fs);
	}

	// TODO(anil): Try to write a Spark app to sample. Should be simpler.

	/**
	 * Creates a single robust tree. As a side effect reads all the sample files
	 * from the samples dir and writes it out WORKING_DIR/sample
	 */
	public void buildRobustTreeFromSamples() {
		assert schemaString != null;
		assert numFields != -1;
		assert numBuckets != -1;
		assert samplesDir != null;
		Schema.createSchema(schemaString, numFields);

		CartilageIndexKeySet sample = readSampleFiles();
		writeOutSample(sample);

		RobustTreeHs index = new RobustTreeHs();
		builder.buildIndexFromSample(
				sample,
				numBuckets,
				index,
				getHDFSWriter(hdfsPartitionDir,
						cfg.getHDFS_REPLICATION_FACTOR()));
	}

	/**
	 * Creates num_replicas robust trees. As a side effect reads all the sample
	 * files from the samples dir and writes it out WORKING_DIR/sample
	 *
	 * @param tpchSize
	 */
	public void buildReplicatedRobustTreeFromSamples() {
		Schema.createSchema(schemaString, numFields);

		CartilageIndexKeySet sample = readSampleFiles();
		writeOutSample(sample);

		builder.buildReplicatedWithSample(
				sample,
				numBuckets,
				key,
				getHDFSWriter(hdfsPartitionDir,
						cfg.getHDFS_REPLICATION_FACTOR()), numFields,
				numReplicas);
	}

	public void writePartitionsFromIndex() {
		Schema.createSchema(schemaString, numFields);

		FileSystem fs = HDFSUtils.getFS(cfg.getHADOOP_HOME()
				+ "/etc/hadoop/core-site.xml");
		byte[] indexBytes = HDFSUtils.readFile(fs, hdfsPartitionDir + "/index");

		// Just load the index. For this we don't need to load the samples.
		RobustTreeHs index = new RobustTreeHs();
		index.unmarshall(indexBytes);

		builder.buildDistributedFromIndex(
				index,
				key,
				inputsDir,
				getHDFSWriter(
						hdfsPartitionDir + "/partitions" + cfg.getMACHINE_ID(),
						cfg.getHDFS_REPLICATION_FACTOR()));
	}

	public void loadSettings(String[] args) {
		int counter = 0;
		while (counter < args.length) {
			switch (args[counter]) {
			case "--inputsDir":
				inputsDir = args[counter + 1];
				counter += 2;
				break;
			case "--samplesDir":
				samplesDir = args[counter + 1];
				counter += 2;
				break;
			case "--schema":
				schemaString = args[counter + 1];
				counter += 2;
				break;
			case "--numFields":
				numFields = Integer.parseInt(args[counter + 1]);
				counter += 2;
				break;
			case "--method":
				method = Integer.parseInt(args[counter + 1]);
				counter += 2;
				break;
			case "--numReplicas":
				numReplicas = Integer.parseInt(args[counter + 1]);
				counter += 2;
				break;
			case "--samplingRate":
				samplingRate = Double.parseDouble(args[counter + 1]);
				counter += 2;
				break;
			case "--numBuckets":
				numBuckets = Integer.parseInt(args[counter + 1]);
				counter += 2;
				break;
			default:
				// Something we don't use
				counter += 2;
				break;
			}
		}
	}

	// Helper function, reads all the sample files and creates a combined
	// sample.
	public CartilageIndexKeySet readSampleFiles() {
		FileSystem fs = HDFSUtils.getFS(cfg.getHADOOP_HOME()
				+ "/etc/hadoop/core-site.xml");

		// read all the sample files and put them into the sample key set
		CartilageIndexKeySet sample = new CartilageIndexKeySet();
		try {
			RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path(
					samplesDir), false);
			while (files.hasNext()) {
				String path = files.next().getPath().toString();
				byte[] bytes = HDFSUtils.readFile(fs, path);
				sample.unmarshall(bytes);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return sample;
	}

	// Helper function, writes out the combined sample file.
	public void writeOutSample(CartilageIndexKeySet sample) {
		FileSystem fs = HDFSUtils.getFS(cfg.getHADOOP_HOME()
				+ "/etc/hadoop/core-site.xml");

		byte[] sampleBytes = sample.marshall();
		OutputStream out = HDFSUtils.getHDFSOutputStream(fs,
				cfg.getHDFS_WORKING_DIR() + "/sample",
				cfg.getHDFS_REPLICATION_FACTOR(), 50 << 20);
		try {
			out.write(sampleBytes);
			out.flush();
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		BenchmarkSettings.loadSettings(args);
		BenchmarkSettings.printSettings();

		RunIndexBuilder t = new RunIndexBuilder();
		t.loadSettings(args);
		t.setUp();

		switch (t.method) {
		case 1:
			t.createSamples();
			break;
		case 2:
			t.buildRobustTreeFromSamples();
			break;
		case 3:
			t.buildReplicatedRobustTreeFromSamples();
			break;
		case 4:
			t.writePartitionsFromIndex();
			break;
		case 5:
			System.out.println("Memory Stats (F/T/M): "
					+ Runtime.getRuntime().freeMemory() + " "
					+ Runtime.getRuntime().totalMemory() + " "
					+ Runtime.getRuntime().maxMemory());
			break;
		default:
			System.out.println("Unknown method " + t.method + " chosen");
			break;
		}
	}
}
