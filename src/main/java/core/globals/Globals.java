package core.globals;

import org.apache.hadoop.fs.FileSystem;

import core.utils.HDFSUtils;

public class Globals {
	// Total number of tuples in dataset.
	public static double TOTAL_NUM_TUPLES = 0;

	// TPC-H datagen generated files use '|'. CSV uses ','.
	public static char DELIMITER = '|';

	// Schema of dataset.
	public static Schema schema = null;

	public static void save(String hdfsPath, short replication, FileSystem fs) {
		String saveContent = "TOTAL_NUM_TUPLES: " + TOTAL_NUM_TUPLES + "\n" +
				"DELIMITER: "  + DELIMITER + "\n" +
				"SCHEMA: " + schema.toString();
		byte[] saveContentBytes = saveContent.getBytes();
		HDFSUtils.writeFile(fs, hdfsPath, replication,
				saveContentBytes, 0, saveContentBytes.length, false);
	}

	public static void load(String hdfsPath, FileSystem fs) {
		byte[] fileContent = HDFSUtils.readFile(fs, hdfsPath);
		String globalContent = new String(fileContent);

		String[] settings = globalContent.split("\n");
		assert settings.length == 3;

		for (int i = 0; i < settings.length; i++) {
			String setting = settings[i];
			String[] parts = setting.split(":");
			switch (parts[0].trim()) {
			case "TOTAL_NUM_TUPLES":
				TOTAL_NUM_TUPLES = Double.parseDouble(parts[1].trim());
				break;
			case "DELIMITER":
				DELIMITER = parts[1].trim().charAt(0);
				break;
			case "SCHEMA":
				schema = Schema.createSchema(parts[1].trim());
				break;
			default:
				System.out.println("Unknown setting found: " + parts[0].trim());
			}
		}
	}
}
