//package perf.benchmark;
//
//import org.apache.curator.framework.CuratorFramework;
//
//import core.index.MDIndex;
//import core.utils.ConfUtils;
//import core.utils.CuratorUtils;
//
//public class BucketCounterManager {
//	ConfUtils cfg;
//
//	CuratorFramework client;
//
//	int method = 0;
//
//	int start = 0;
//
//	int end = 0;
//
//	public BucketCounterManager() {
//		cfg = new ConfUtils(BenchmarkSettings.conf);
//	}
//
//	public void sumCounters(int start, int end) {
//		MDIndex.BucketCounts bc = new MDIndex.BucketCounts(cfg.getZOOKEEPER_HOSTS());
//		long total = 0;
//		for (int i = start; i < end; i++) {
//			long bucketCount = bc.getBucketCount(i);
//			System.out.println("" + i + ":" + bucketCount);
//			total += bucketCount;
//		}
//
//		System.out.println("Total: " + total);
//  	}
//
//	public void deleteCounters() {
//		CuratorFramework client =
//			CuratorUtils.createAndStartClient(cfg.getZOOKEEPER_HOSTS());
//		CuratorUtils.deleteAll(client, "/", "partition-count-");
//		client.close();
//	}
//
//	public void loadSettings(String[] args) {
//		int counter = 0;
//		while (counter < args.length) {
//			switch (args[counter]) {
//			case "--method":
//				method = Integer.parseInt(args[counter + 1]);
//				counter += 2;
//				break;
//			case "--start":
//				start = Integer.parseInt(args[counter + 1]);
//				counter += 2;
//				break;
//			case "--end":
//				end = Integer.parseInt(args[counter + 1]);
//				counter += 2;
//				break;
//			default:
//				// Something we don't use
//				counter += 2;
//				break;
//			}
//		}
//	}
//
//	public static void main(String[] args) {
//		BenchmarkSettings.loadSettings(args);
//		BenchmarkSettings.printSettings();
//
//		BucketCounterManager bc = new BucketCounterManager();
//		bc.loadSettings(args);
//		switch (bc.method) {
//		case 0:
//			bc.sumCounters(bc.start, bc.end);
//			break;
//		case 1:
//			bc.deleteCounters();
//			break;
//		}
//	}
//}
