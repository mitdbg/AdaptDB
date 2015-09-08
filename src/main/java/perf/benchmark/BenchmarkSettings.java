package perf.benchmark;


public class BenchmarkSettings {
	public static String pathToDataset = "/Users/anil/Dev/repos/tpch-dbgen/sf_2/";
	public static String conf = "/Users/anil/Dev/repos/mdindex/conf/cartilage.properties";

	public static void loadSettings(String[] args) {
		int counter = 0;
		while (counter < args.length) {
			switch (args[counter]) {
			case "--pathToDataset":
				pathToDataset = args[counter + 1];
				counter += 2;
				break;
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
		System.out.println("Path to dataset: " + pathToDataset);
		System.out.println("Conf: " + conf);
	}
}