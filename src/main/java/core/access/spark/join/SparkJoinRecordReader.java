package core.access.spark.join;

import java.io.IOException;
import java.util.Iterator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.common.collect.ArrayListMultimap;

import core.access.ReusableHDFSPartition;
import core.access.iterator.IteratorRecord;
import core.access.iterator.ReusablePartitionIterator;
import core.access.spark.SparkInputFormat.SparkFileSplit;
import core.access.spark.join.SparkJoinRecordReader.JoinTuplePair;
import core.utils.BufferManager;
import core.utils.Pair;

public class SparkJoinRecordReader extends
		RecordReader<LongWritable, JoinTuplePair> {

	protected Configuration conf;

	protected SparkFileSplit sparkSplit;
	int currentFile;

	protected ReusablePartitionIterator iterator;
	protected BufferManager buffMgr;

	LongWritable key;
	long recordId;
	boolean hasNext;

	CuratorFramework client;
	long relationId;

	ArrayListMultimap<Long, byte[]> hashTable;
	int joinAttribute1, joinAttribute2;

	boolean secondInputFirstRecord = false;
	Iterator<byte[]> firstRecords;
	byte[] secondRecord;
	JoinTuplePair value;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {

		System.out.println("Initializing SparkRecordReader");
		buffMgr = new BufferManager();

		conf = context.getConfiguration();
		// client =
		// CuratorUtils.createAndStartClient(conf.get(SparkQueryConf.ZOOKEEPER_HOSTS));
		client = null;
		sparkSplit = (SparkFileSplit) split;

		iterator = (ReusablePartitionIterator) sparkSplit.getIterator();
		currentFile = 0;

		hasNext = initializeNext();
		key = new LongWritable();
		value = new JoinTuplePair(new IteratorRecord(), new IteratorRecord());
		recordId = 0;

		hashTable = ArrayListMultimap.create();

		String joinCond = conf.get("JOIN_CONDITION");
		String tokens[] = joinCond.split("=");
		// rid1 = Integer.parseInt(tokens[0].split("\\.")[0]);
		joinAttribute1 = Integer.parseInt(tokens[0].split("\\.")[1]);
		// rid2 = Integer.parseInt(tokens[1].split("\\.")[0]);
		joinAttribute2 = Integer.parseInt(tokens[1].split("\\.")[1]);

		buildPhase();
	}

	protected void buildPhase() throws IOException, InterruptedException {
		long firstRelation = -1;
		while (getNext()) {
			if (firstRelation == -1)
				firstRelation = relationId;
			if (firstRelation == relationId) {
				IteratorRecord r = iterator.next();
				hashTable.put(r.getLongAttribute(joinAttribute1), r.getBytes());
			} else {
				recordId = 0;
				secondInputFirstRecord = true; // indicate that we have already
												// fetched the first record of
												// the second input
				break; // finished the first input
			}
		}
	}

	protected boolean initializeNext() throws IOException {

		if (currentFile > 0) {
			System.out.println("Records read = " + recordId);
			// System.gc();
		}

		if (currentFile >= sparkSplit.getStartOffsets().length)
			return false;
		else {
			Path filePath = sparkSplit.getPath(currentFile);
			final FileSystem fs = filePath.getFileSystem(conf);
			ReusableHDFSPartition partition = new ReusableHDFSPartition(fs,
					filePath.toString(), client, buffMgr);
			System.out.println("loading path: " + filePath.toString());
			try {
				partition.loadNext();
				iterator.setPartition(partition);
				currentFile++;
				relationId = sparkSplit.getPath(currentFile - 1).toString()
						.contains("repl") ? 1 : 0; // CHECK
				return true;
			} catch (java.lang.OutOfMemoryError e) {
				System.out
						.println("ERR: Failed to load " + filePath.toString());
				System.out.println(e.getMessage());
				e.printStackTrace();
				return false;
			}
		}
	}

	protected boolean getNext() throws IOException {
		while (hasNext) {
			if (iterator.hasNext()) {
				recordId++;
				return true;
			}
			hasNext = initializeNext();
		}
		return false;
	}

	public boolean nextKeyValue() throws IOException, InterruptedException {

		if (firstRecords != null && firstRecords.hasNext())
			return true;

		while (secondInputFirstRecord || getNext()) {
			secondInputFirstRecord = false;
			IteratorRecord r = iterator.next();
			long key = r.getLongAttribute(joinAttribute2);
			if (hashTable.containsKey(key)) {
				firstRecords = hashTable.get(key).iterator();
				secondRecord = r.getBytes();
				return true;
			}
		}

		return false;
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		key.set(recordId);
		// key.set(relationId);
		return key;
	}

	@Override
	public JoinTuplePair getCurrentValue() throws IOException,
			InterruptedException {
		value.first.setBytes(firstRecords.next());
		value.second.setBytes(secondRecord);
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return (float) currentFile / sparkSplit.getStartOffsets().length;
	}

	@Override
	public void close() throws IOException {
		iterator.finish(); // this method could even be called earlier in case
							// the entire split does not fit in main-memory
	}

	public static class JoinTuplePair extends
			Pair<IteratorRecord, IteratorRecord> {
		public JoinTuplePair(IteratorRecord first, IteratorRecord second) {
			super(first, second);
		}
	}
}
