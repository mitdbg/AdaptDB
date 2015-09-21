package core.access.iterator;

/**
 * Created by qui on 5/20/15.
 */

import core.index.key.CartilageIndexKey;

import java.io.Serializable;

/**
 * An wrapper class over CartilageIndexKey (to reuse much of the functionality)
 *
 * @author alekh
 *
 */
public class IteratorRecord extends CartilageIndexKey implements Serializable {

	/**
     *
     */
	private static final long serialVersionUID = 1L;

	public IteratorRecord() {
		// super('|');
		super(PartitionIterator.delimiter);
	}

	public IteratorRecord(int[] keyAttrIdx) {
		super(PartitionIterator.delimiter, keyAttrIdx);
	}

	public byte[] getBytes() {
		return this.bytes;
	}

	public int getOffset() {
		return this.offset;
	}

	public int getLength() {
		return this.length;
	}
}
