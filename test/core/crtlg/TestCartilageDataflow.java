package core.crtlg;

import junit.framework.TestCase;
import core.index.MDIndex;
import core.index.SimpleRangeTree;
import core.index.key.CartilageIndexKey;
import core.index.key.MDIndexKey;

public class TestCartilageDataflow extends TestCase{

	CartilageDataflow d;
	
	MDIndex mdIndex;
	MDIndexKey mdIndexKey;
	
	String inputPath;
	String hdfsPath;
	int bucketSize;
	
	
	public void setUp(){
		mdIndexKey = new CartilageIndexKey();	// partition all attributes
		inputPath = "/Users/alekh/Work/Cartilage/support/datasets/scale_1/lineitem.tbl";
		hdfsPath = "testPath";
		bucketSize = 1024*1024*10;	// 10MB
		d = new CartilageDataflow("/Users/alekh/Work/Cartilage/MDIndex/conf/cartilage.properties");
	}
	
	public void testSimpleRangeTree(){
		mdIndex = new SimpleRangeTree(100);
		d.run(mdIndex, mdIndexKey, inputPath, hdfsPath, bucketSize);
		mdIndex.initProbe();
	}
	
	public void tearDown(){
	}

}
