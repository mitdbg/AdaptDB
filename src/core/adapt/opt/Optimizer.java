package core.adapt.opt;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import core.adapt.Predicate;
import core.adapt.Query;
import core.adapt.Query.FilterQuery;
import core.index.key.CartilageIndexKey;
import core.index.robusttree.RobustTree;

public class Optimizer {
	RobustTree rt;
	String dataset;

	static final int BLOCK_SIZE = 64 * 1024;

	public Optimizer(String dataset) {
		this.dataset = dataset;
	}

	// TODO: Someone should fill into the bucket object the number of tuples it contains
	public boolean buildIndex() {
		File file = new File(this.dataset);
		if (file.exists()) {
			long bytes = file.length();

			// Round up the number of nearest power of 2
			int numBlocks = (int) (bytes/BLOCK_SIZE);
			numBlocks = roundTo2(numBlocks);

			this.rt = new RobustTree(numBlocks);

			CartilageIndexKey key;
			key = new CartilageIndexKey('|', new int[]{0,1,2,3,4,5});

			BufferedReader br;
			try {
				br = new BufferedReader(new FileReader(dataset));
				String line;
				while ((line = br.readLine()) != null) {
					key.setBytes(line.getBytes());
					rt.insert(key);
				}
				br.close();
				rt.initProbe();

				return true;
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		return false;
	}

	public void buildPlan(Query q, int numWorkers) {
		if (q instanceof FilterQuery) {
			FilterQuery fq = (FilterQuery) q;
			Predicate[] p = fq.getPredicates();
		} else {
			System.err.println("Unimplemented query - Unable to build plan");
		}
	}

	public static int roundTo2(int numBlocks) {
		int k = 31 - Integer.numberOfLeadingZeros(numBlocks);
		if (numBlocks == (int)Math.pow(2, k)) {
			return numBlocks;
		} else {
			return (int) Math.pow(2, k+1);
		}
	}
}
