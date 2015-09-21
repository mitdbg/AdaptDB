package core.index.build;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.mutable.MutableInt;

import com.google.common.collect.Maps;

public abstract class PartitionWriter implements Cloneable{
	protected int bufferPartitionSize = 1024*1024;
	protected String partitionDir;

	protected Map<String, OutputStream> buffer;
	protected Map<String, MutableInt> partitionRecordCount;
	private long writingTime = 0;


	public PartitionWriter(String partitionDir, int bufferPartitionSize) {
		this(partitionDir);
		this.bufferPartitionSize = bufferPartitionSize;
	}

	public PartitionWriter(String partitionDir) {
		this.partitionDir = partitionDir;
		this.buffer = Maps.newHashMap();
		this.partitionRecordCount = Maps.newHashMap();
	}

	@Override
	public PartitionWriter clone() throws CloneNotSupportedException {
		PartitionWriter w = (PartitionWriter) super.clone();
		w.buffer = Maps.newHashMap();
		w.partitionRecordCount = Maps.newHashMap();
        return w;
	}

	public void setPartitionDir(String partitionDir) {
		this.partitionDir = partitionDir;
	}

	public String getPartitionDir() {
		return this.partitionDir;
	}

	public void writeToPartition(String partitionId, byte[] bytes, int b_offset, int b_length) {
		long start = System.nanoTime();
		OutputStream b = buffer.get(partitionId);
		if (b==null) {
			// if there is a hard limit on the number of buffers, then close some of them before opening new ones!
			//if(buffer.size() > maxBufferPartitions)
			//	flush((int)(flushFraction*maxBufferPartitions));

			b = getOutputStream(partitionDir+"/"+partitionId);
			buffer.put(partitionId, b);
		}

		try {
			b.write(bytes, b_offset, b_length);
			b.write('\n');

			// update the counters
			MutableInt count = partitionRecordCount.get(partitionId);
			if (count == null)
				partitionRecordCount.put(partitionId, new MutableInt());
			else
				count.increment();
			writingTime += System.nanoTime()-start;

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	protected abstract OutputStream getOutputStream(String path);

	public abstract void createPartitionDir();

	public void flush(){
		flush(buffer.size());
	}

	protected void flush(int numPartitions){
		System.out.println("SCAN: partition writing time = "+writingTime/1E9);
		long start = System.nanoTime();
		int flushCount = 0;
		Set<String> keys = new HashSet<String>(buffer.keySet());
		for(String k: keys){
			try {
				buffer.get(k).close();
				buffer.remove(k);
			} catch (IOException e) {
				e.printStackTrace();
			}
			flushCount++;
			if(flushCount > numPartitions)
				break;
		}
		System.out.println("SCAN: flushing time = "+(System.nanoTime()-start)/1E9);
	}
}
