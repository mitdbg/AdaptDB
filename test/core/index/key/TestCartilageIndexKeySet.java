package core.index.key;

import java.util.Iterator;
import java.util.List;

import junit.framework.TestCase;
import core.index.Settings;

public class TestCartilageIndexKeySet extends TestCase{

	private CartilageIndexKey key;
	private CartilageIndexKeySet keyset;


	int[] keyIds;
	String tuple1, tuple2;
	String datafile;

	@Override
	public void setUp(){
		datafile = Settings.tpchPath + "lineitem.tbl";

		tuple1 = "1|1552|93|1|17|24710.35|0.04|0.02|N|O|1996-03-13|1996-02-12|1996-03-22|DELIVER IN PERSON|TRUCK|egular courts above the";
		tuple2 = "1|674|75|2|36|56688.12|0.09|0.06|N|O|1996-04-12|1996-02-28|1996-04-20|TAKE BACK RETURN|MAIL|ly final dependencies: slyly bold ";

		keyIds = new int[]{2,3};
		key = new CartilageIndexKey('|', keyIds);
		keyset = new CartilageIndexKeySet();
	}

	private void doInsert(){
		key.setBytes(tuple1.getBytes());
		keyset.insert(key);
		key.setBytes(tuple2.getBytes());
		keyset.insert(key);
		printKeysetValues();
	}

	private void doCheck(Object[] obj, String tuple){
		String[] attrs = tuple.split("\\|");
		assertEquals(obj.length, keyIds.length);
		for(int i=0;i<keyIds.length;i++)
			assertEquals(obj[i].toString(), attrs[keyIds[i]]);
	}

	private void printKeysetValues(){
		System.out.println();
		for(Object[] values: keyset.getValues()){
			for(Object obj: values)
				System.out.print(obj+",");
			System.out.println();
		}
	}

	public void testInsert(){
		doInsert();

		List<Object[]> values = keyset.getValues();
		assertEquals(values.size(), 2);

		doCheck(values.get(0), tuple1);
		doCheck(values.get(1), tuple2);

		//System.out.println(values);
	}

	public void testSort(){
		doInsert();

		keyset.sort(0);
		printKeysetValues();
		List<Object[]> values = keyset.getValues();
		doCheck(values.get(0), tuple2);
		doCheck(values.get(1), tuple1);

		keyset.reset();
		doInsert();

		keyset.sort(1);
		printKeysetValues();
		values = keyset.getValues();
		doCheck(values.get(0), tuple1);
		doCheck(values.get(1), tuple2);
	}

	public void testIterate(){
		doInsert();

		System.out.println();
		Iterator<CartilageIndexKey> itr = keyset.iterator();
		while(itr.hasNext()){
			CartilageIndexKey k = itr.next();
			System.out.println(k.getKeyString());
		}
	}
}
