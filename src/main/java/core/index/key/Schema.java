package core.index.key;

import org.junit.Assert;

import core.utils.TypeUtils.TYPE;

/**
 * Defines the schema of the table. Needs to populated first.
 *
 * @author anil
 *
 */
public class Schema {
	public static Schema schema = null;

	public Field[] fields;

	public static class Field {
		String name;
		TYPE type;
		int id;

		public static int maxId = 0;

		public Field(String name, TYPE type) {
			this.name = name;
			this.type = type;
			this.id = maxId;

			maxId += 1;
		}
	}

	public Schema(Field[] fields) {
		this.fields = fields;
	}

	public static void createSchema(String schemaString, int numFields) {
		Field[] fieldList = new Field[numFields];
		String[] columns = schemaString.split(",");
		assert columns.length == numFields;
		assert schema == null;
		for (int i = 0; i < columns.length; i++) {
			String[] columnInfo = columns[i].trim().split(" ");
			String fieldName = columnInfo[0].trim();
			// enum valueOf is case sensitive, make everything uppercase.
			TYPE fieldType = TYPE.valueOf(columnInfo[1].trim().toUpperCase());
			fieldList[i] = new Field(fieldName, fieldType);
		}
		schema = new Schema(fieldList);
	}

	public static int getAttributeId(String attrName) {
		Assert.assertNotNull(schema);
		for (Field f: schema.fields) {
			if (f.name.equals(attrName)) {
				return f.id;
			}
		}

		return -1;
	}

	public static TYPE getType(int attrId) {
		Assert.assertNotNull(schema);
		Assert.assertEquals(schema.fields[attrId].id, attrId);
		return schema.fields[attrId].type;
	}
}
