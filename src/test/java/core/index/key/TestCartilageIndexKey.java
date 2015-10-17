package core.index.key;

import java.text.SimpleDateFormat;

import junit.framework.TestCase;
import core.utils.TypeUtils.SimpleDate;
import core.utils.TypeUtils.TYPE;

public class TestCartilageIndexKey extends TestCase {

	private TYPE[] types;
	private String tuple1;
	// private CartilageBinaryRecord r1;
	private CartilageIndexKey key;

	public static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

	@Override
	public void setUp() {
		tuple1 = "1|1552|93|1|17|24710.35|0.04|0.02|N|O|1996-03-13|1996-02-12|1996-03-22|DELIVER IN PERSON|TRUCK|egular courts above the";
		types = new TYPE[] { TYPE.INT, TYPE.INT, TYPE.INT, TYPE.INT, TYPE.INT,
				TYPE.DOUBLE, TYPE.DOUBLE, TYPE.DOUBLE, TYPE.STRING,
				TYPE.STRING, TYPE.DATE, TYPE.DATE, TYPE.DATE, TYPE.STRING,
				TYPE.STRING, TYPE.VARCHAR };

		// r1 = new CartilageBinaryRecord('|');
		// r1.setBytes(tuple1.getBytes());

		key = new CartilageIndexKey('|');
	}

	public void testSetTuple() {
		key.setBytes(tuple1.getBytes());
		assert (true);
	}

	public void testSetTupleVarchar() {
		key.setBytes(tuple1.getBytes());
		TYPE[] keyTypes = key.types;
		for (int i = 0; i < types.length; i++) {
			System.out.println(types[i] + " " + keyTypes[i]);
			if (types[i] != TYPE.VARCHAR)
				assertEquals(types[i], keyTypes[i]);
		}
	}

	public void testGetKeyString() {
		key.setBytes(tuple1.getBytes());
		// key.detectTypes();
		assertEquals(tuple1, key.getKeyString());
	}

	public void testGetStringAttribute() {
		key.setBytes(tuple1.getBytes());
		// key.detectTypes();
		String expected = tuple1.split("\\|")[8];
		assertEquals(expected, key.getStringAttribute(8, 20));
	}

	public void testGetIntAttribute() {
		key.setBytes(tuple1.getBytes());
		// key.detectTypes();
		int expected = Integer.parseInt(tuple1.split("\\|")[1]);
		assertEquals(expected, key.getIntAttribute(1));
	}

	public void testGetLongAttribute() {
		key.setBytes(tuple1.getBytes());
		// key.detectTypes();
		long expected = Long.parseLong(tuple1.split("\\|")[1]);
		assertEquals(expected, key.getLongAttribute(1));
	}

	public void testGetFloatAttribute() {
		key.setBytes(tuple1.getBytes());
		// key.detectTypes();
		float expected = Float.parseFloat(tuple1.split("\\|")[5]);
		assertEquals(expected, key.getFloatAttribute(5));
	}

	public void testGetDoubleAttribute() {
		key.setBytes(tuple1.getBytes());
		// key.detectTypes();
		double expected = Double.parseDouble(tuple1.split("\\|")[5]);
		double actual = key.getDoubleAttribute(5);

		assertEquals(Math.round(expected * 1000) / 1000,
				Math.round(actual * 1000) / 1000);
	}

	public void testGetDateAttribute() {
		key.setBytes(tuple1.getBytes());
		String[] tokens = tuple1.split("\\|")[10].split("-");
		SimpleDate d = new SimpleDate(Integer.parseInt(tokens[0]),
				Integer.parseInt(tokens[1]), Integer.parseInt(tokens[2]));

		assertEquals(d, key.getDateAttribute(10));
	}

	@Override
	public void tearDown() {
	}
}
