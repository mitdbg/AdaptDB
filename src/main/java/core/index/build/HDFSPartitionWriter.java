package core.index.build;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import core.index.MDIndex;
import core.utils.ConfUtils;
import core.utils.HDFSUtils;

public class HDFSPartitionWriter extends PartitionWriter {

	private ConfUtils conf;

	private short replication = 3;
	private FileSystem hdfs;

	public HDFSPartitionWriter(String partitionDir, int bufferPartitionSize,
			short replication, ConfUtils cfg) {
		super(partitionDir, bufferPartitionSize);
		this.replication = replication;
		createHDFS(cfg);
	}

	public HDFSPartitionWriter(String partitionDir, ConfUtils cfg) {
		super(partitionDir);
		createHDFS(cfg);
	}

	@Override
	public PartitionWriter clone() throws CloneNotSupportedException {
		HDFSPartitionWriter w = (HDFSPartitionWriter) super.clone();
		w.createHDFS(this.conf);
		return w;
	}

	private void createHDFS(ConfUtils cfg) {
		this.conf = cfg;
		this.hdfs = HDFSUtils.getFS(this.conf.getHADOOP_HOME()
				+ "/etc/hadoop/core-site.xml");
	}

	@Override
	public void createPartitionDir() {
		try {
			hdfs.mkdirs(new Path(partitionDir));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void deletePartitionDir() {
		try {
			hdfs.delete(new Path(partitionDir), true);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	protected OutputStream getOutputStream(String path) {
		return HDFSUtils.getHDFSOutputStream(hdfs, path, replication,
				bufferPartitionSize);
	}

	@Override
	public void flush() {
		MDIndex.BucketCounts c = new MDIndex.BucketCounts(
				conf.getZOOKEEPER_HOSTS());
		for (String k : buffer.keySet())
			try {
				c.setToBucketCount(Integer.parseInt(k), partitionRecordCount
						.get(k).intValue());
			} catch (NumberFormatException e) {

			}
		c.close();
		super.flush();
	}
}
