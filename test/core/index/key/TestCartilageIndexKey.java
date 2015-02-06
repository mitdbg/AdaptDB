package core.index.key;

import java.text.ParseException;
import java.util.Date;

import junit.framework.TestCase;
import core.data.CartilageDatum.CartilageBinaryRecord;
import core.utils.SchemaUtils.TYPE;

public class TestCartilageIndexKey extends TestCase{

	private TYPE[] types;
	private String tuple1;
	private CartilageBinaryRecord r1;
	private CartilageIndexKey key;
	
	public void setUp(){
		tuple1 = "1|1552|93|1|17|24710.35|0.04|0.02|N|O|1996-03-13|1996-02-12|1996-03-22|DELIVER IN PERSON|TRUCK|egular courts above the";
		types = new TYPE[]{TYPE.INT, TYPE.INT, TYPE.INT, TYPE.INT, TYPE.INT, TYPE.FLOAT, TYPE.FLOAT, TYPE.FLOAT, 
				TYPE.STRING, TYPE.STRING, TYPE.DATE, TYPE.DATE, TYPE.DATE, TYPE.STRING, TYPE.STRING, TYPE.VARCHAR};
		
		r1 = new CartilageBinaryRecord('|');
		r1.setBytes(tuple1.getBytes());
		
		key = new CartilageIndexKey();
	}
	
	public void testSetTuple(){
		key.setTuple(r1);
		assert(true);
	}
	
	public void testSetTupleVarchar(){
		key.setTuple(r1);
		TYPE[] keyTypes = key.detectTypes();
		for(int i=0;i<types.length;i++){
			System.out.println(types[i]+" "+keyTypes[i]);
			if(types[i]!=TYPE.VARCHAR)
				assertEquals(types[i], keyTypes[i]);
		}
	}
	
	public void testGetKeyString() {
		key.setTuple(r1);
		key.detectTypes();
		assertEquals(tuple1, key.getKeyString());
	}

	public void testGetStringAttribute() {
		key.setTuple(r1);
		key.detectTypes();
		String expected = tuple1.split("\\|")[8];
		assertEquals(expected, key.getStringAttribute(8, 100));
	}

	public void testGetIntAttribute() {
		key.setTuple(r1);
		key.detectTypes();
		int expected = Integer.parseInt(tuple1.split("\\|")[1]);
		assertEquals(expected, key.getIntAttribute(1));
	}

	public void testGetLongAttribute() {
		key.setTuple(r1);
		key.detectTypes();
		long expected = Long.parseLong(tuple1.split("\\|")[1]);
		assertEquals(expected, key.getLongAttribute(1));
	}

	public void testGetFloatAttribute() {
		key.setTuple(r1);
		key.detectTypes();
		float expected = Float.parseFloat(tuple1.split("\\|")[5]);
		assertEquals(expected, key.getFloatAttribute(5));
	}
	
	public void testGetDoubleAttribute() {
		key.setTuple(r1);
		key.detectTypes();
		double expected = Double.parseDouble(tuple1.split("\\|")[5]);
		double actual = key.getDoubleAttribute(5);
		
		assertEquals(Math.round(expected*1000)/1000, Math.round(actual*1000)/1000);
	}
	
	public void testGetDateAttribute(){
		key.setTuple(r1);
		key.detectTypes();
		try {
			Date d = CartilageIndexKey.sdf.parse(tuple1.split("\\|")[10]);
			assertEquals(d, key.getDateAttribute(10));
		} catch (ParseException e) {
			throw new RuntimeException("could not parse date");
		}
	}
	
	
	public void tearDown(){
	}
}
