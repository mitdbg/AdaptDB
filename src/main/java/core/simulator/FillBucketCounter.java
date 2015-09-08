package core.simulator;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.Scanner;

import core.utils.ConfUtils;
import perf.benchmark.BenchmarkSettings;
import core.index.MDIndex.BucketCounts;

public class FillBucketCounter {
	public static void main(String[] args) {
		System.out.println("Enter mode:");
		Scanner in = new Scanner(System.in);
		String input = in.nextLine().trim();
	    ConfUtils cfg = new ConfUtils(BenchmarkSettings.conf);
	    String zookeeperHosts = cfg.getZOOKEEPER_HOSTS();
		if (input.equals("put")) {
			try {
				System.out.println("Enter file name:");
				String BUCKET_DATA_FILE = in.nextLine().trim();
			    if (zookeeperHosts != null) {
					BucketCounts c = new BucketCounts(zookeeperHosts);
					BufferedReader br = new BufferedReader(new FileReader(BUCKET_DATA_FILE));
					String line;
				    while ((line = br.readLine()) != null) {
				    	String[] splits = line.split("\\s+");
				    	int bid = Integer.parseInt(splits[0]);
				    	int val = Integer.parseInt(splits[1]);
						c.removeBucketCount(bid);
				    	c.setToBucketCount(bid, val);
				    }
				    br.close();
				} else {
					System.out.println("INFO: Zookeeper Hosts NULL");
				}
			} catch (Exception e) {
				e.printStackTrace();
			}	
		} else if (input.equals("get")) {
			try {
				System.out.println("Enter file name:");
				String BUCKET_DATA_FILE = in.nextLine().trim();
				System.out.println("Enter max bid:");
				int maxBID = Integer.parseInt(in.nextLine());
			    if (zookeeperHosts != null) {
					BucketCounts c = new BucketCounts(zookeeperHosts);
					PrintWriter writer = new PrintWriter(BUCKET_DATA_FILE, "UTF-8");
				    for (int bid=0; bid<maxBID; bid++) {
				    	int count = c.getBucketCount(bid);
				    	writer.println(bid + " " + count);
				    }
					writer.close();
				} else {
					System.out.println("INFO: Zookeeper Hosts NULL");
				}
			} catch (Exception e) {
				e.printStackTrace();
			}				
		} else {
			System.out.println("Unknown mode");
		}
		
		in.close();
	}
}
