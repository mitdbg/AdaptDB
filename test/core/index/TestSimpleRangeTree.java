package core.index;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import junit.framework.TestCase;
import core.index.key.CartilageIndexKey;

public class TestSimpleRangeTree extends TestCase {

	private SimpleRangeTree t;
	private int bucketSize;

	private CartilageIndexKey key;
	//private CartilageBinaryRecord r;

	String tuple1, tuple2;
	String datafile;

	@Override
	public void setUp(){
		datafile = Settings.tpchPath + "lineitem.tbl";

		tuple1 = "1|1552|93|1|17|24710.35|0.04|0.02|N|O|1996-03-13|1996-02-12|1996-03-22|DELIVER IN PERSON|TRUCK|egular courts above the";
		tuple2 = "1|674|75|2|36|56688.12|0.09|0.06|N|O|1996-04-12|1996-02-28|1996-04-20|TAKE BACK RETURN|MAIL|ly final dependencies: slyly bold ";

		t = new SimpleRangeTree(100);
		key = new CartilageIndexKey('|', new int[]{0,1,2,3,4,5});
		bucketSize = 1024*1024*10;	// 10mb

		//r = new CartilageBinaryRecord('|');
	}

	public void testInitBuild(){
		t.initBuild(bucketSize);
		assert(true);
	}

	public void testInsert(){
		key.setBytes(tuple1.getBytes());
		t.insert(key);
		assert(true);
	}

	public void testInitProbe(){
		key.setBytes(tuple1.getBytes());
		t.insert(key);

		key.setBytes(tuple2.getBytes());
		t.insert(key);

		t.initProbe();
	}

	public void testInitProbeBulk(){
		BufferedReader br;
		try {
			br = new BufferedReader(new FileReader(datafile));
			String line;
			while ((line = br.readLine()) != null) {
				key.setBytes(line.getBytes());
				//key.setTuple(r);
				t.insert(key);
			}
			br.close();
			t.initProbe();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void testGetBucketId(){
	}

	@Override
	public void tearDown(){
	}
}
