package core.index.build;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;

public class BufferedPartitionWriter extends PartitionWriter{

	public BufferedPartitionWriter(String partitionDir, int bufferPartitionSize, int maxBufferPartitions){
		super(partitionDir, bufferPartitionSize, maxBufferPartitions);
	}
	
	public BufferedPartitionWriter(String partitionDir) {
		super(partitionDir);
	}

	protected OutputStream getOutputStream(String path){
		try {
			return new BufferedOutputStream(new FileOutputStream(path, true), bufferPartitionSize);
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Failed to create an output stream!");
		}
	}	
}
