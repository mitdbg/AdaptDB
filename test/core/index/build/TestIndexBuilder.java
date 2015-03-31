package core.index.build;

import java.io.File;

import junit.framework.TestCase;
import core.index.Settings;
import core.index.SimpleRangeTree;
import core.index.kdtree.KDMedianTree;
import core.index.key.CartilageIndexKey;

public class TestIndexBuilder extends TestCase{

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
		numPartitions = 100;

		localPartitionDir = Settings.localPartitionDir;
		hdfsPartitionDir = Settings.hdfsPartitionDir;
		propertiesFile = Settings.cartilageConf;

		key = new CartilageIndexKey('|');
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
