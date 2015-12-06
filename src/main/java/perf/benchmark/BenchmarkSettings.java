package perf.benchmark;

import core.common.globals.Globals;

public class BenchmarkSettings {
	//public static String conf = "/Users/ylu/Documents/workspace/mdindex/conf/ylu.properties";
	public static String conf = "/home/mdindex/yilu/mdindex/conf/tpch.properties";

	public static void loadSettings(String[] args) {
		Globals.DELIMITER = '|';

		int counter = 0;
		while (counter < args.length) {
			switch (args[counter]) {
			case "--conf":
				conf = args[counter + 1];
				counter += 2;
				break;
			case "--numTuples":
				Globals.TOTAL_NUM_TUPLES = Double.parseDouble(args[counter + 1]);
				counter += 2;
				break;
			case "--delimiter":
				Globals.DELIMITER = args[counter + 1].trim().charAt(0);
				counter += 2;
				break;
			default:
				// Something we don't use
				counter += 2;
				break;
			}
		}
	}

	public static void printSettings() {
		System.out.println("Conf: " + conf);
	}
}