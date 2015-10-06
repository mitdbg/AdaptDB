package core.index.build;

import junit.framework.TestCase;
import core.index.MDIndex.Bucket;
import core.index.robusttree.RNode;
import core.index.robusttree.RobustTree;
import core.utils.TypeUtils.TYPE;

public class TestIndexSerialization extends TestCase {
	@Override
	public void setUp() {

	}

	public void testSerialization() {
		RNode root = new RNode();
		root.attribute = 0;
		root.type = TYPE.INT;
		root.value = 3;

		RNode l = new RNode();
		RNode r = new RNode();

		root.leftChild = l;
		root.rightChild = r;

		l.attribute = 1;
		l.type = TYPE.DOUBLE;
		l.value = 3.2;

		r.attribute = 2;
		r.type = TYPE.STRING;
		r.value = "bad";

		l.leftChild = new RNode();
		l.leftChild.bucket = new Bucket();

		r.leftChild = new RNode();
		r.leftChild.bucket = new Bucket();

		l.rightChild = new RNode();
		l.rightChild.bucket = new Bucket();

		r.rightChild = new RNode();
		r.rightChild.bucket = new Bucket();

		RobustTree t = new RobustTree();
		t.setRoot(root);

		t.maxBuckets = 4;
		t.numAttributes = 4;
		t.dimensionTypes = new TYPE[] { TYPE.INT, TYPE.DOUBLE, TYPE.STRING,
				TYPE.LONG };

		byte[] treeBytes = t.marshall();

		RobustTree clone = new RobustTree();
		clone.unmarshall(treeBytes);

		assert (t == clone);
	}
}
