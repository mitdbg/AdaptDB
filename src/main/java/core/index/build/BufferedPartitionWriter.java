package core.index.build;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.io.FileUtils;

public class BufferedPartitionWriter extends PartitionWriter {

	public BufferedPartitionWriter(String partitionDir, int bufferPartitionSize) {
		super(partitionDir, bufferPartitionSize);
	}

	public BufferedPartitionWriter(String partitionDir) {
		super(partitionDir);
	}

	@Override
	protected OutputStream getOutputStream(String path) {
		try {
			return new BufferedOutputStream(new FileOutputStream(path, true),
					bufferPartitionSize);
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Failed to create an output stream!");
		}
	}

	@Override
	public void createPartitionDir() {
		try {
			FileUtils.forceMkdir(new File(partitionDir));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
