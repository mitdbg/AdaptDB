package core.common.globals;

import org.junit.Assert;

import core.utils.TypeUtils.TYPE;

/**
 * Defines the schema of the table. Needs to populated first.
 *
 * @author anil
 *
 */
public class Schema {
	public Field[] fields;

	public static class Field {
		public String name;
		public TYPE type;
		int id;

		public Field(String name, TYPE type, int id) {
			this.name = name;
			this.type = type;
			this.id = id;

		}

		@Override
		public String toString() {
			return this.name + " " + this.type.toString();
		}
	}

	public Schema(Field[] fields) {
		this.fields = fields;
	}

	@Override
	public String toString() {
		String ret = "";
		for (int i = 0; i < this.fields.length; i++) {
			ret += this.fields[i].toString();
			if (i != this.fields.length - 1) {
				ret += ",";
			}
		}
		return ret;
	}

	public static Schema createSchema(String schemaString) {
		String[] columns = schemaString.split(",");
		Field[] fieldList = new Field[columns.length];

		for (int i = 0; i < columns.length; i++) {
			String[] columnInfo = columns[i].trim().split(" ");
			String fieldName = columnInfo[0].trim();
			// enum valueOf is case sensitive, make everything uppercase.
			TYPE fieldType = TYPE.valueOf(columnInfo[1].trim().toUpperCase());
			fieldList[i] = new Field(fieldName, fieldType, i);
		}

		System.out.println("INFO: Created schema with " + fieldList.length + " fields");
		return new Schema(fieldList);
	}

	public int getAttributeId(String attrName) {
		for (Field f: fields) {
			if (f.name.equals(attrName)) {
				return f.id;
			}
		}

		return -1;
	}

	public TYPE getType(int attrId) {
		Assert.assertEquals(fields[attrId].id, attrId);
		return fields[attrId].type;
	}
}
