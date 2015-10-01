package core.utils;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.fs.FileSystem;

/**
 * Custom HDFS Output Stream implementation. Buffers the writes and writes out
 * the buffer once full. Differs from using OutputStream with connection to HDFS
 * in that it doesn't maintain a persistent connection.
 *
 * @author anil
 *
 */
public class HDFSBufferedOutputStream extends OutputStream {
	// Buffer.
	byte[] buffer;

	// Current write pointer in the buffer.
	// curPointer == buffer.length is a valid state => buffer is full.
	int curPointer = 0;

	// Path to HDFS file.
	String filePath;

	// HDFS Filesystem.
	FileSystem fs;

	public HDFSBufferedOutputStream(FileSystem fs, String filePath,
			short replication, int bufferSize) {
		this.buffer = new byte[bufferSize];
		this.filePath = filePath;
		this.fs = fs;

		// Create the file if it does not exist.
		HDFSUtils.safeCreateFile(this.fs, filePath, replication);
	}

	@Override
	public void flush() {
		HDFSUtils.appendBytes(this.fs, this.filePath, this.buffer, 0,
				curPointer);
		this.curPointer = 0;
	}

	@Override
	public void write(int b) throws IOException {
		if (curPointer + 4 + 1 > buffer.length) {
			this.flush();
		}

		buffer[curPointer] = (byte) ((b >> 24) & 0xFF);
		buffer[curPointer + 1] = (byte) ((b >> 16) & 0xFF);
		buffer[curPointer + 2] = (byte) ((b >> 8) & 0xFF);
		buffer[curPointer + 3] = (byte) (b & 0xFF);

		curPointer += 4;
	}

	@Override
	public void write(byte[] b) throws IOException {
		if (curPointer + b.length + 1 > buffer.length) {
			this.flush();
		}

		System.arraycopy(b, 0, buffer, curPointer, b.length);
		curPointer += b.length;
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		if (curPointer + len + 1 > buffer.length) {
			this.flush();
		}

		System.arraycopy(b, off, buffer, curPointer, len);
		curPointer += len;
	}

	@Override
	public void close() {
		if (curPointer > 0) {
			this.flush();
		}
	}
}