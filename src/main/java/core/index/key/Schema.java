package core.index.key;

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

		public Field(String name, TYPE type) {
			this.name = name;
			this.type = type;
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
}
