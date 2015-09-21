package core.access.iterator;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import core.access.HDFSPartition;
import core.access.Partition;
import core.index.robusttree.RNode;

public class DistributedPartitioningIterator implements Serializable {

	private static final long serialVersionUID = 1L;

	protected Map<Integer, Partition> newPartitions = new HashMap<Integer, Partition>();
	protected Map<Integer, Partition> oldPartitions = new HashMap<Integer, Partition>();

	private Iterator<String> iterator;
	private transient RNode newIndexTree;
	protected String zookeeperHosts;
	private String propertiesFile;
	private String hdfsPath;

	protected Partition partition;
	protected IteratorRecord record;

	public static class LocalPartition extends HDFSPartition {

		private static final long serialVersionUID = 1L;
		private Random r;

		public LocalPartition(String pathAndPartitionId, String propertiesFile,
				short replication) {
			super(pathAndPartitionId, propertiesFile, replication);
			bytes = new byte[1024 * 1024];
			r = new Random(System.currentTimeMillis());
		}

		public void store(boolean append) {
			String storePath = path + "/" + partitionId + "/" + r.nextInt();
			if (!path.startsWith("hdfs"))
				storePath = "/" + storePath;
			Path e = new Path(storePath);
			FSDataOutputStream os;
			try {
				if (append && hdfs.exists(e)) {
					os = hdfs.append(e);
				} else {
					os = hdfs.create(new Path(storePath), replication);
				}
				os.write(bytes, 0, offset);
				os.flush();
				os.close();
			} catch (IOException ex) {
				throw new RuntimeException(ex.getMessage());
			}
			// HDFSUtils.writeFile(hdfs, storePath, replication, bytes, 0,
			// offset, append);
		}
	}

	public DistributedPartitioningIterator(String zookeeperHosts,
			RNode newIndexTree, String propertiesFile, String path) {
		this.zookeeperHosts = zookeeperHosts;
		this.newIndexTree = newIndexTree;
		record = new IteratorRecord();
		this.propertiesFile = propertiesFile;
		this.hdfsPath = path;
	}

	public void setIterator(Iterator<String> iterator) {
		this.iterator = iterator;
	}

	public void setPartition(Partition p) {
		partition = p;
	}

	public boolean hasNext() {

		if (iterator.hasNext()) {
			record.setBytes(iterator.next().getBytes());

			int id = newIndexTree.getBucketId(record);
			Partition p;
			if (newPartitions.containsKey(id)) {
				p = newPartitions.get(id);
			} else {
				p = new LocalPartition(propertiesFile, hdfsPath + "/0",
						(short) 1);
				// p = partition.clone();
				p.setPartitionId(id);
				newPartitions.put(id, p);
			}
			// p.write(record.getBytes(), record.getOffset(),
			// record.getLength());
			p.write(record.getBytes(), 0, record.getBytes().length);

			return true;
		}

		return false;
	}

	protected boolean isRelevant(IteratorRecord record) {
		return true;
	}

	public void finish() {
		for (Partition p : newPartitions.values())
			p.store(true);
	}

	private void writeObject(java.io.ObjectOutputStream out) throws IOException {
		out.defaultWriteObject();
		String tree = newIndexTree.marshall();
		Text.writeString(out, tree);
	}

	private void readObject(java.io.ObjectInputStream in) throws IOException,
			ClassNotFoundException {
		in.defaultReadObject();
		String tree = Text.readString(in);
		newIndexTree = new RNode();
		newIndexTree.unmarshall(tree.getBytes());
	}
}
