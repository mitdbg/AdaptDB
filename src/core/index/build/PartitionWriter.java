package core.index.build;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;

public abstract class PartitionWriter{

	protected int bufferPartitionSize = 5*1024*1024;
	protected int maxBufferPartitions = 100;
	protected String partitionDir;
	
	protected Map<String,OutputStream> buffer;
	
	public PartitionWriter(String partitionDir, int bufferPartitionSize, int maxBufferPartitions){
		this(partitionDir);
		this.bufferPartitionSize = bufferPartitionSize;
		this.maxBufferPartitions = maxBufferPartitions;
	}
	
	public PartitionWriter(String partitionDir) {
		this.partitionDir = partitionDir;
		this.buffer = Maps.newHashMap();
	}

	public void writeToPartition(String partitionId, byte[] bytes, int b_offset, int b_length){
		OutputStream b = buffer.get(partitionId);
		if(b==null){
			// if there is a hard limit on the number of buffers, then close some of them before opening new ones!
			//if(buffer.size() > maxBufferPartitions)
			//	flush((int)(flushFraction*maxBufferPartitions));
			
			b = getOutputStream(partitionDir+"/"+partitionId);
			buffer.put(partitionId, b);
		}
		
		try {
			b.write(bytes, b_offset, b_length);
			b.write('\n');
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	protected abstract OutputStream getOutputStream(String path);
	
	public void flush(){
		flush(buffer.size());		
	}
	
	protected void flush(int numPartitions){
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
	}
}