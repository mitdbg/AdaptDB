package perf.benchmark;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import core.globals.Globals;
import core.globals.Schema;
import core.index.RobustTree;
import core.index.build.HDFSPartitionWriter;
import core.index.build.IndexBuilder;
import core.index.build.PartitionWriter;
import core.key.RawIndexKey;
import core.key.ParsedTupleList;
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
	RawIndexKey key;
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
	double samplingRate = 0.0;

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

		builder = new IndexBuilder();
		key = new RawIndexKey(Globals.DELIMITER);
	}

	private PartitionWriter getHDFSWriter(String partitionDir, short replication) {
		return new HDFSPartitionWriter(partitionDir, partitionBufferSize,
				replication, this.cfg);
	}

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
	 * Loads the globals from the /info file.
	 * Available after the index has been written out to HDFS.
	 */
	public void loadGlobals() {
		Globals.load(cfg.getHDFS_WORKING_DIR() + "/info",
				HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME()));
	}

	/**
	 * Creates one sample file sample.machineId and writes it out to HDFS.
	 *
	 * @param machineId
	 *            descriptor for machine the code will run on.
	 */
	public void createSamples() {
		assert numFields != -1;
		assert inputsDir != null;

		FileSystem fs = HDFSUtils.getFS(cfg.getHADOOP_HOME()
				+ "/etc/hadoop/core-site.xml");

		// If the input sampingRate = 0.0 (unspecified), then calculate it automatically
		if (samplingRate == 0.0){
			samplingRate = calculateSampingRate(inputsDir);
			System.out.println("The input samplingRate = 0.0, we set it to " + samplingRate);
		}

		builder.blockSampleInput(
				samplingRate,
				key,
				inputsDir,
				cfg.getHDFS_WORKING_DIR() + "/samples/sample."
						+ cfg.getMACHINE_ID(), fs);
	}

	/**
	 * samplingRate = 1GB / sizeof (totalInputFileSize);
	 *
	 * @param inputDirectory
	 */
	private double calculateSampingRate(String inputDirectory) {
		File[] files = new File(inputDirectory).listFiles();
		long totalFileSize = 0;
		for (File f : files) {
			totalFileSize += f.length();
		}
		long oneGB =  1L << 30;
		double rate = 1.0;
		if (oneGB < totalFileSize) {
			rate = 1.0 * oneGB / totalFileSize;
		}
		return rate;
	}

	// TODO(anil): Try to write a Spark app to sample. Should be simpler.

	/**
	 * Creates a single robust tree. As a side effect reads all the sample files
	 * from the samples dir and writes it out WORKING_DIR/sample
	 */
	public void buildRobustTreeFromSamples() {
		assert numFields != -1;
		assert numBuckets != -1;
		assert samplesDir != null;

		FileSystem fs = HDFSUtils.getFS(cfg.getHADOOP_HOME()
				+ "/etc/hadoop/core-site.xml");

		// Write out the combined sample file.
		ParsedTupleList sample = readSampleFiles();
		writeOutSample(fs, sample);

		// Write out global settings for this dataset.
		Globals.save(cfg.getHDFS_WORKING_DIR() + "/info",
				cfg.getHDFS_REPLICATION_FACTOR(), fs);

		// Construct the index from the sample.
		RobustTree index = new RobustTree();
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
		FileSystem fs = HDFSUtils.getFS(cfg.getHADOOP_HOME()
				+ "/etc/hadoop/core-site.xml");

		// Write out the combined sample file.
		ParsedTupleList sample = readSampleFiles();
		writeOutSample(fs, sample);

		// Write out global settings for this dataset.
		Globals.save(cfg.getHDFS_WORKING_DIR() + "/info",
				cfg.getHDFS_REPLICATION_FACTOR(), fs);

		builder.buildReplicatedWithSample(
				sample,
				numBuckets,
				key,
				getHDFSWriter(hdfsPartitionDir,
						cfg.getHDFS_REPLICATION_FACTOR()), numFields,
				numReplicas);
	}

	/**
	 * Creates num_replicas robust trees. As a side effect reads all the sample
	 * files from the samples dir and writes it out WORKING_DIR/sample
	 *
	 * @param tpchSize
	 */
	public void writeOutSampleFile() {
		FileSystem fs = HDFSUtils.getFS(cfg.getHADOOP_HOME()
				+ "/etc/hadoop/core-site.xml");

		// Write out the combined sample file.
		ParsedTupleList sample = readSampleFiles();
		writeOutSample(fs, sample);
	}	
	
	public void writePartitionsFromIndex() {
		FileSystem fs = HDFSUtils.getFS(cfg.getHADOOP_HOME()
				+ "/etc/hadoop/core-site.xml");
		byte[] indexBytes = HDFSUtils.readFile(fs, hdfsPartitionDir + "/index");

		// Just load the index. For this we don't need to load the samples.
		RobustTree index = new RobustTree();
		index.unmarshall(indexBytes);

		// TODO(anil): Make this the name of dataset.
		String dataDir = "/data";
		builder.buildDistributedFromIndex(
				index,
				key,
				inputsDir,
				getHDFSWriter(
						hdfsPartitionDir + dataDir + "/partitions" + cfg.getMACHINE_ID(),
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
				Globals.schema = Schema.createSchema(schemaString);
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
	public ParsedTupleList readSampleFiles() {
		FileSystem fs = HDFSUtils.getFS(cfg.getHADOOP_HOME()
				+ "/etc/hadoop/core-site.xml");

		// read all the sample files and put them into the sample key set
		ParsedTupleList sample = new ParsedTupleList();
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
	public void writeOutSample(FileSystem fs, ParsedTupleList sample) {
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
		case 6:
			t.writeOutSampleFile();
			break;
		default:
			System.out.println("Unknown method " + t.method + " chosen");
			break;
		}
	}
}
