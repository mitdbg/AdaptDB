package core.adapt.opt;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import core.adapt.Predicate;
import core.adapt.Query;
import core.adapt.Query.FilterQuery;
import core.index.key.CartilageIndexKey;
import core.index.robusttree.RNode;
import core.index.robusttree.RobustTree;
import core.utils.SchemaUtils.TYPE;

public class Optimizer {
	RobustTree rt;
	int rtDepth;

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
			rtDepth = getDepthOfIndex(numBlocks);
			numBlocks = (int) Math.pow(2, rtDepth);

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
			List<RNode> buckets = rt.getMatchingBuckets(p);

			getBestPlan(buckets, p);
		} else {
			System.err.println("Unimplemented query - Unable to build plan");
		}
	}

	public static class LevelNode {
		RNode origin; // The bucket
		RNode previous;
		RNode current;
		int status;
	}

	public static class Plan {

	}

	public Plan getBestPlan(List<RNode> buckets, Predicate[] ps) {
		// Create a list of level node
		// Level node is the parent of bucket at the current level
		List<LevelNode> nodes = new ArrayList<LevelNode>();
		for (RNode node: buckets) {
			LevelNode l = new LevelNode();
			l.origin = node;
			l.previous = node;
			l.current = node.parent;
			nodes.add(l);
		}

		//TODO: Multiple predicates seem to complicate the simple idea we had; think more :-/
		Plan plan = null;
		for (Predicate p: ps) {
			Plan option = getBestPlanForPredicate(nodes, p);
			if (plan == null) {
				plan = option;
			} else {
				plan = option;
			}

			for (LevelNode l: nodes) {
				l.current = l.origin.parent;
			}
		}

		return plan;
	}

	public boolean checkValid(RNode node, int attrId, TYPE t, Object val) {
		// TODO:
		return true;
	}

	/**
	 * Puts the new estimated number of tuples in each bucket after change
	 * @param changed
	 */
	public void populateBucketEstimates(RNode changed) {

	}

	/**
	 * Gives the number of tuples accessed
	 * @param changed
	 * @return
	 */
	public int getNumTuplesAccessed(RNode changed) {

		return 0;
	}

	/**
	 * Replaces node old by node r in the tree
	 * @param old
	 * @param r
	 */
	public void replaceInTree(RNode old, RNode r) {
        old.leftChild.parent = r;
        old.rightChild.parent = r;
        if (old.parent.rightChild == old) {
        	old.parent.rightChild = r;
        } else {
        	old.parent.leftChild = r;
        }
	}

	public Plan getBestPlanForPredicate(List<LevelNode> nodes, Predicate p) {
		while (true) {
			for (int i=0; i<nodes.size(); i++) {
				if (i < nodes.size() - 1) {
					if (nodes.get(i).current == nodes.get(i+1).current) {
						RNode old = nodes.get(i).current;
						if (checkValid(old, p.attribute, p.type, p.value)) {
							RNode r = old.clone();
							r.attribute = p.attribute;
							r.type = p.type;
							r.value = p.value;
							replaceInTree(old, r);

					        populateBucketEstimates(r);
					        int numAccessedOld = 0;
					        int numAcccessedNew = getNumTuplesAccessed(r);
					        int benefit = numAccessedOld - numAcccessedNew;

					        if (benefit > 0) {
					        	// compute cost
					        }

					        // Restore
					        replaceInTree(r, old);
						}
					} else {
						RNode old = nodes.get(i).current;

						// Reached a choke point
						if (old.attribute == p.attribute) {

						} else {
							// No go - this is the end of our journey
							// This can happen when we have multiple predicates
						}
					}
				}
			}
		}
	}

	public static int getDepthOfIndex(int numBlocks) {
		int k = 31 - Integer.numberOfLeadingZeros(numBlocks);
		if (numBlocks == (int)Math.pow(2, k)) {
			return k;
		} else {
			return k+1;
		}
	}
}
