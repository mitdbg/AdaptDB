package perf.benchmark;

public class BenchmarkSettings {
	//public static String conf = "/Users/ylu/Documents/workspace/mdindex/conf/ylu.properties";
	public static String conf = "/home/mdindex/yilu/mdindex/conf/tpch.properties";

	public static void loadSettings(String[] args) {
		int counter = 0;
		while (counter < args.length) {
			switch (args[counter]) {
			case "--conf":
				conf = args[counter + 1];
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