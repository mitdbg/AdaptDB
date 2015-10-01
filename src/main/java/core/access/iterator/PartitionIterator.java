package core.access.iterator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.lang.ArrayUtils;

import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;

import core.access.Partition;
import core.access.Predicate;
import core.utils.BinaryUtils;
import core.utils.ReflectionUtils;

public class PartitionIterator implements Iterator<IteratorRecord> {

	public static enum ITERATOR {
		SCAN, FILTER, REPART
	};

	protected IteratorRecord record;
	protected byte[] recordBytes;
	protected byte[] brokenRecordBytes;

	protected static char newLine = '\n';

	protected byte[] bytes;
	protected int bytesLength, offset, previous;

	protected Partition partition;

	protected Predicate[] predicates;

	public PartitionIterator() {
	}

	public PartitionIterator(String itrString) {
	}

	public void setPartition(Partition partition) {
		this.partition = partition;
		record = new IteratorRecord();
		bytes = partition.getNextBytes();
		// bytesLength = partition.getSize();
		bytesLength = bytes == null ? 0 : bytes.length;
		offset = 0;
		previous = 0;
		brokenRecordBytes = null;
	}

	@Override
	public boolean hasNext() {
		for (; offset < bytesLength; offset++) {
			if (bytes[offset] == newLine) {
				// record.setBytes(bytes, previous, offset-previous);
				recordBytes = ArrayUtils.subarray(bytes, previous, offset);
				if (brokenRecordBytes != null) {
					recordBytes = BinaryUtils.concatenate(brokenRecordBytes,
							recordBytes);
					brokenRecordBytes = null;
				}
				try {
					record.setBytes(recordBytes);
				} catch (ArrayIndexOutOfBoundsException e) {
					System.out
							.println("Index out of bounds while setting bytes: "
									+ (new String(recordBytes)));
					throw e;
				}
				previous = ++offset;
				if (isRelevant(record)) {
					// System.out.println("relevant record found ..");
					return true;
				} else
					continue;
			}
		}

		if (previous < bytesLength)
			brokenRecordBytes = BinaryUtils.getBytes(bytes, previous,
					bytesLength - previous);
		else
			brokenRecordBytes = null;

		bytes = partition == null ? null : partition.getNextBytes();
		if (bytes != null) {
			bytesLength = bytes.length;
			offset = 0;
			previous = 0;
			return hasNext();
		} else
			return false;
	}

	protected boolean isRelevant(IteratorRecord record) {
		return true;
	}

	@Override
	public void remove() {
		next();
	}

	@Override
	public IteratorRecord next() {
		return record;
	}

	public void finish() {
	}

	public void write(DataOutput out) throws IOException {
	}

	public void readFields(DataInput in) throws IOException {
	}

	// public static PartitionIterator read(DataInput in) throws IOException {
	// PartitionIterator it = new PartitionIterator();
	// it.readFields(in);
	// return it;
	// }

	public static String iteratorToString(PartitionIterator itr) {
		ByteArrayDataOutput dat = ByteStreams.newDataOutput();
		try {
			itr.write(dat);
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("Failed to serialize the partitioner");
		}
		return itr.getClass().getName() + "@" + new String(dat.toByteArray());
	}

	public static PartitionIterator stringToIterator(String itrString) {
		String[] tokens = itrString.split("@", 2);
		return (PartitionIterator) ReflectionUtils.getInstance(tokens[0],
				new Class<?>[] { String.class }, new Object[] { tokens[1] });
	}
}
