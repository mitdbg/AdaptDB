package core.index.build;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.commons.io.FileUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import core.utils.IOUtils;

public class ByteMappedPartitionWriter extends PartitionWriter {

	private Map<String, byte[]> buffer;
	private Map<String, Integer> bufferOffset;

	public ByteMappedPartitionWriter(String partitionDir,
			int bufferPartitionSize) {
		super(partitionDir, bufferPartitionSize);
		this.buffer = Maps.newHashMap();
		this.bufferOffset = Maps.newHashMap();
	}

	public ByteMappedPartitionWriter(String partitionDir) {
		super(partitionDir);
		this.buffer = Maps.newHashMap();
		this.bufferOffset = Maps.newHashMap();
	}

	@Override
	public void writeToPartition(String partitionId, byte[] bytes,
			int b_offset, int b_length) {
		Integer offset = bufferOffset.get(partitionId);
		if (offset == null) {
			// if(buffer.size() >= maxBufferPartitions) // flush flushFraction
			// of the partitions
			// flush((int)(flushFraction*maxBufferPartitions));
			buffer.put(partitionId, new byte[bufferPartitionSize]);
			bufferOffset.put(partitionId, 0);
			offset = 0;
		} else if (offset + b_length + 1 >= bufferPartitionSize) {
			appendByteArray(partitionDir + "/" + partitionId,
					buffer.get(partitionId), 0, bufferOffset.get(partitionId));
			bufferOffset.put(partitionId, 0);
			offset = 0;
		}

		// write the bytes to buffer[partitionId] at position
		// bufferOffset[partitionId]
		System.arraycopy(bytes, b_offset, buffer.get(partitionId), offset,
				b_length);
		buffer.get(partitionId)[offset + b_length] = '\n';

		// increment bufferOffset[partitionId] by the written number of bytes
		bufferOffset.put(partitionId, offset + b_length + 1);
	}

	@Override
	protected OutputStream getOutputStream(String path) {
		throw new UnsupportedOperationException(
				"no output streams in this method");
	}

	@Override
	public void createPartitionDir() {
		try {
			FileUtils.forceMkdir(new File(partitionDir));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	protected void flush(int numPartitions) {
		// sort the partitions by their size
		List<Entry<String, Integer>> l = Lists.newArrayList(bufferOffset
				.entrySet());
		Collections.sort(l, new Comparator<Entry<String, Integer>>() {
			@Override
			public int compare(Entry<String, Integer> o1,
					Entry<String, Integer> o2) {
				return o2.getValue().compareTo(o1.getValue());
			}
		});

		// flush the numPartitions number of largest partitions in memory
		for (int i = 0; i < numPartitions; i++) {
			String key = l.get(i).getKey();
			appendByteArray(partitionDir + "/" + key, buffer.get(key), 0,
					bufferOffset.get(key));
			buffer.remove(key);
			bufferOffset.remove(key);
		}
	}

	public void appendByteArray(String filename, byte[] bytes, int offset,
			int size) {
		try {
			OutputStream os = new FileOutputStream(filename, true);
			os.write(bytes, offset, size);
			os.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void writeIOUtils(String filename, byte[] bytes) {
		IOUtils.appendByteArray(filename, bytes);
	}

	static OutputStream os;

	public static void writeBufferedWrite(String filename, byte[] bytes) {
		try {
			if (os == null)
				os = new BufferedOutputStream(new FileOutputStream(filename,
						true));
			os.write(bytes);
			// IOUtils.closeOutputStream(os);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void writeFileOS(String filename, byte[] bytes) {
		try {
			OutputStream os = new FileOutputStream(filename, true);
			os.write(bytes);
			// fos.flush();
			os.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		String filename = "/Users/alekh/test";
		int byteSize = 10 * 1024 * 1024;
		int bufferWrites = 100;

		byte[] bytes = new byte[byteSize];
		FileUtils.deleteQuietly(new File(filename));

		Random r = new Random(System.currentTimeMillis());

		long start = System.nanoTime();
		for (int i = 0; i < bufferWrites; i++) {

			bytes[r.nextInt(byteSize)] = 1;

			// writeIOUtils(filename, bytes);
			writeBufferedWrite(filename, bytes);
			// writeFileOS(filename, bytes);
		}

		IOUtils.closeOutputStream(os);
		System.out.println("Time take = " + (System.nanoTime() - start) / 1E9);

	}

}
