package core.access.iterator;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import core.adapt.Predicate;
import core.adapt.ReusableHDFSPartition;
import core.adapt.Predicate.PREDTYPE;
import core.adapt.Query;
import core.adapt.iterator.IteratorRecord;
import core.adapt.iterator.ReusablePartitionIterator;
import core.utils.BufferManager;
import core.utils.ConfUtils;
import core.utils.HDFSUtils;
import core.utils.TypeUtils.TYPE;
import junit.framework.TestCase;

public class TestHDFSScanIteratorReuse extends TestCase {

	protected String partitionDir;
	protected List<String> partitionPaths;

	protected Query query;
	String propertiesFile;

	BufferManager buffMgr;
	Multimap<Long, byte[]> hashTable = ArrayListMultimap.create();

	// Map<Long,Integer> keyCard = Maps.newHashMap();
	// Map<Long, List<byte[]>> hashTable = Maps.newHashMap();

	@Override
	public void setUp() {
		propertiesFile = "/Users/alekh/Work/MDIndex/conf/cartilage.properties";
		partitionDir = "hdfs://localhost:9000/user/alekh/dodo";
		int attributeIdx = 0;
		// Range r = RangeUtils.closed(3000000, 6000000);
		Predicate p1 = new Predicate(attributeIdx, TYPE.INT, 3000000,
				PREDTYPE.GEQ);
		Predicate p2 = new Predicate(attributeIdx, TYPE.INT, 6000000,
				PREDTYPE.LEQ);
		query = new Query("asd", new Predicate[] { p1, p2 });

		partitionPaths = Lists.newArrayList();
		ConfUtils cfg = new ConfUtils(propertiesFile);
		FileSystem hdfs = HDFSUtils.getFS(cfg.getHADOOP_HOME()
				+ "/etc/hadoop/core-site.xml");
		try {
			for (FileStatus fileStatus : hdfs
					.listStatus(new Path(partitionDir))) {
				String name = fileStatus.getPath().getName();
				try {
					Integer.parseInt(name);
					if (fileStatus.isFile()
							&& !fileStatus.getPath().getName().startsWith("."))
						partitionPaths.add("hdfs://localhost:9000"
								+ fileStatus.getPath().toUri().getPath()); // Do
																			// something
																			// with
																			// child
				} catch (Exception e) {
				}
			}
		} catch (IOException e) {
			System.out.println("No files to repartition");
		}

		buffMgr = new BufferManager();
	}

	protected ReusableHDFSPartition getPartitionInstance(String path) {
		return new ReusableHDFSPartition(path, propertiesFile, (short) 1,
				buffMgr);
	}

	public void testScan() {
		System.out.println("Scanning: " + partitionDir + "/0");
		ReusableHDFSPartition partition = getPartitionInstance(partitionDir
				+ "/0");
		partition.load();
		ReusablePartitionIterator itr = new ReusablePartitionIterator();
		itr.setPartition(partition);

		int recCount = 0;
		while (itr.hasNext()) {
			itr.next();
			recCount++;
		}
		System.out.println("Number of records = " + recCount);
	}

	private void insertIntoHashTable(long key, byte[] value) {
		// Integer card = keyCard.get(key);
		// if(card==null || card>=5){ // do we need to create a new value array?
		// byte[][] valueArr = new byte[5][];
		// valueArr[0] = value;
		// hashTable.put(key, valueArr);
		// keyCard.put(key, 1);
		// }
		// else{
		// List<byte[][]> l = hashTable.get(key);
		// l.get(l.size()-1)[card] = value;
		// keyCard.put(key, card+1);
		// }

		hashTable.put(key, value);
		// if(!hashTable.containsKey(key))
		// hashTable.put(key, new ArrayList<byte[]>());
		// hashTable.get(key).add(value);
	}

	private void printHashTable() {
		System.out.println(hashTable.size());
	}

	public void testScanAll() {
		// ReusableHDFSPartition partition = getPartitionInstance("");
		ReusablePartitionIterator itr = new ReusablePartitionIterator();

		Runtime rt = Runtime.getRuntime();
		int recCount = 0;
		long startTime = System.nanoTime();

		for (String partitionPath : partitionPaths) {
			System.out.println("Scanning: " + partitionPath);
			// partition.setPathAndPartitionId(partitionPath);
			ReusableHDFSPartition partition = new ReusableHDFSPartition(
					partitionPath, propertiesFile, (short) 1, buffMgr);
			partition.load();
			itr.setPartition(partition);

			long usedMB = (rt.totalMemory() - rt.freeMemory()) / 1024 / 1024;
			System.out.println("memory usage: " + usedMB + " MB");
			while (itr.hasNext()) {
				IteratorRecord r = itr.next();
				recCount++;
				// String s = r.getKeyString();
				// System.out.println(s);
				// System.out.println(s.length());
				insertIntoHashTable(r.getLongAttribute(0), r.getBytes());
			}

			usedMB = (rt.totalMemory() - rt.freeMemory()) / 1024 / 1024;
			System.out.println("memory usage: " + usedMB + " MB");
		}
		System.out.println("Number of records = " + recCount);
		double time = (System.nanoTime() - startTime) / 1E9;
		System.out.println("Scan time: " + time + " secs");
		printHashTable();
	}
}
