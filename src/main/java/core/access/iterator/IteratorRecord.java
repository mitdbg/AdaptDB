package core.access.iterator;

/**
 * Created by qui on 5/20/15.
 */

import java.io.Serializable;

import core.index.key.CartilageIndexKey;
import core.index.robusttree.Globals;

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
		super(Globals.DELIMITER);
	}

	public IteratorRecord(int[] keyAttrIdx) {
		super(Globals.DELIMITER, keyAttrIdx);
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
