package perf.benchmark;

import core.common.globals.Globals;
import core.common.globals.TableInfo;
import core.common.index.RobustTree;
import core.common.key.ParsedTupleList;
import core.common.key.RawIndexKey;
import core.upfront.build.HDFSPartitionWriter;
import core.upfront.build.IndexBuilder;
import core.upfront.build.PartitionWriter;
import core.utils.ConfUtils;
import core.utils.CuratorUtils;
import core.utils.HDFSUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Builds the index. Captures time taken by the different steps in index
 * building.
 * Make sure that the table info has been created.
 * TODO: Compute sampling fraction instead of taking as an option.
 *
 * @author anil
 *
 */
public class RunIndexBuilder {
	RawIndexKey key;
	IndexBuilder builder;

	int partitionBufferSize;

	ConfUtils cfg;

	// Table name.
	String tableName;
	
	// Directory on local file system containing the inputs.
	String inputsDir;

	// Specifies which method should be run.
	// See 'main' for method numbers.
	int method = -1;

	// Sampling probability.
	double samplingRate = 0.0;

	// Number of buckets in the index.
	int numBuckets = -1;

	// Directory corresponding to table on HDFS.
	String tableHDFSDir;

	// HDFS Filesystem.
	FileSystem fs;

    // Table Info.
    TableInfo tableInfo;

    // Zookeeper client.
    CuratorFramework client;

	public void setUp() {

		partitionBufferSize = 2 * 1024 * 1024;

		cfg = new ConfUtils(BenchmarkSettings.conf);
		fs = HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME());

		// Load table info.
		Globals.loadTableInfo(tableName, cfg.getHDFS_WORKING_DIR(), fs);
		tableInfo = Globals.getTableInfo(tableName);
		assert tableInfo != null;

		builder = new IndexBuilder();
		key = new RawIndexKey(tableInfo.delimiter);
		
		tableHDFSDir = cfg.getHDFS_WORKING_DIR() + "/" + tableName;

        client = CuratorUtils.createAndStartClient(
                cfg.getZOOKEEPER_HOSTS());
	}

	private PartitionWriter getHDFSWriter(String partitionDir, short replication) {
		return new HDFSPartitionWriter(partitionDir, partitionBufferSize,
				replication, this.cfg, client);
	}

	/**
	 * Creates one sample file sample.machineId and writes it out to HDFS.
	 */
	public void createSamples() {
		assert inputsDir != null;

		// If the input samplingRate = 0.0 (unspecified), then calculate it automatically
		if (samplingRate == 0.0) {
			samplingRate = calculateSampingRate(inputsDir);
			System.out.println("The input samplingRate = 0.0, we set it to " + samplingRate);
		}

		builder.blockSampleInput(
				samplingRate,
				key,
				inputsDir,
				tableHDFSDir + "/samples/sample." + cfg.getMACHINE_ID(), 
				fs);
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
		assert numBuckets != -1;

		// Write out the combined sample file.
		ParsedTupleList sample = readSampleFiles();
		writeOutSample(fs, sample);

		// Construct the index from the sample.
		RobustTree index = new RobustTree(tableInfo);
		builder.buildIndexFromSample(
				sample,
				numBuckets,
				index,
				getHDFSWriter(tableHDFSDir,
						cfg.getHDFS_REPLICATION_FACTOR()));
	}


	/**
	 * Creates a single robust tree(hard code the attributes used in the tree for now).
	 * As a side effect reads all the sample files
	 * from the samples dir and writes it out WORKING_DIR/sample
	 */
	public void buildJoinRobustTreeFromSamples() {
		assert numBuckets != -1;

		// Write out the combined sample file.
		ParsedTupleList sample = readSampleFiles();
		writeOutSample(fs, sample);

		//lineitem

		//int[] dim = {0,0,0,0,0,0,0,12,12,12,14};

		// orders

		int[] dim = {0,0,0,0,0,0,0,4};

		// Construct the index from the sample.
		RobustTree index = new RobustTree(tableInfo);
		builder.buildJoinIndexFromSample(
				sample,
				numBuckets,dim,
				index,
				getHDFSWriter(tableHDFSDir,
						cfg.getHDFS_REPLICATION_FACTOR()));
	}


	/**
	 * Creates num_replicas robust trees. As a side effect reads all the sample
	 * files from the samples dir and writes it out WORKING_DIR/sample
	 */
	public void writeOutSampleFile() {
		FileSystem fs = HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME());

		// Write out the combined sample file.
		ParsedTupleList sample = readSampleFiles();
		writeOutSample(fs, sample);
	}	
	
	public void writePartitionsFromIndex() {
		FileSystem fs = HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME());
		byte[] indexBytes = HDFSUtils.readFile(fs, tableHDFSDir + "/index");

		// Just load the index. For this we don't need to load the samples.
		RobustTree index = new RobustTree(tableInfo);
		index.unmarshall(indexBytes);

		String dataDir = "/data";
		builder.buildDistributedFromIndex(
				index,
				key,
				inputsDir,
				getHDFSWriter(
					cfg.getHDFS_WORKING_DIR() + "/" + tableName + dataDir,
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
			case "--tableName":
				tableName = args[counter+1];
				counter += 2;
				break;
			case "--method":
				method = Integer.parseInt(args[counter + 1]);
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
		FileSystem fs = HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME());

		// read all the sample files and put them into the sample key set
		ParsedTupleList sample = new ParsedTupleList(tableInfo.getTypeArray());
		try {
			RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path(
					tableHDFSDir + "/samples/"), false);
			while (files.hasNext()) {
				String path = files.next().getPath().toString();
				byte[] bytes = HDFSUtils.readFile(fs, path);
				sample.unmarshall(bytes, tableInfo.delimiter);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return sample;
	}

	// Helper function, writes out the combined sample file.
	public void writeOutSample(FileSystem fs, ParsedTupleList sample) {
		byte[] sampleBytes = sample.marshall(tableInfo.delimiter);
		OutputStream out = HDFSUtils.getHDFSOutputStream(fs,
				tableHDFSDir + "/sample",
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
			t.buildJoinRobustTreeFromSamples();
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
