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
			Predicate[] ps = fq.getPredicates();
//			List<RNode> buckets = rt.getMatchingBuckets(ps);

//			getBestPlan(ps);
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
		public Action actions; // Tree of actions
		public float cost;
		public float benefit;
	}

	public static class Action {
		public int option; // Says which if I used to create this plan
		public Action left;
		public Action right;
	}

	public static class Plans {
		boolean fullAccess;
		Plan PTop;
		Plan Best;

		public Plans() {
			this.fullAccess = false;
			this.PTop = null;
			this.Best = null;
		}
	}

	public Plan getBestPlan(Predicate[] ps) {
		//TODO: Multiple predicates seem to complicate the simple idea we had; think more :-/
		Plan plan = null;
		for (Predicate p: ps) {
			Plan option = getBestPlanForPredicate(p);
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

	public Plan getBestPlanForPredicate(Predicate p) {
		RNode root = rt.getRoot();
		Plans ps = getBestPlanForSubtree(root, p);
		return ps.Best;
	}

	// Update the dest with source if source is a better plan
	public void updatePlan(Plan dest, Plan source) {
		boolean copy = false;
		if (dest.cost == -1) {
			copy = true;
		}

		// TODO: Write the comparison condition

		if (copy) {
			dest.benefit = source.benefit;
			dest.cost = source.cost;
			dest.actions = source.actions;
		}

	}

	public Plans getBestPlanForSubtree(RNode node, Predicate p) {
		if (node.bucket != null) {
			// Leaf
			Plans pl = new Plans();
			pl.fullAccess = true;

			return pl;
		} else {
			Plan pTop = new Plan();
			Plan best = new Plan();

			Plans leftPlan = getBestPlanForSubtree(node.rightChild, p);
			Plans rightPlan = getBestPlanForSubtree(node.rightChild, p);

			// replace attribute by one in the predicate
			if (leftPlan.fullAccess && rightPlan.fullAccess) {
				// If we traverse to root and see that there is no node with cutoff point less than
				// that of predicate, we can do this
				if (checkValid(node, p.attribute, p.type, p.value)) {
					RNode r = node.clone();
					r.attribute = p.attribute;
					r.type = p.type;
					r.value = p.value;
					replaceInTree(node, r);

			        populateBucketEstimates(r);
			        int numAccessedOld = 0;
			        int numAcccessedNew = getNumTuplesAccessed(r);
			        float benefit = (float) numAccessedOld - numAcccessedNew;

			        if (benefit > 0) {
			        	// compute cost
			        	float cost = 0;
			        	Plan p1 = new Plan();
			        	p1.cost = cost;
			        	p1.benefit = benefit;
			        	Action a1 = new Action();
			        	a1.option = 1;
			        	p1.actions = a1;

			        	updatePlan(pTop, p1);
			        	updatePlan(best, p1);
			        }

			        // Restore
			        replaceInTree(r, node);
				}
			}

			// Swap down the attribute and bring p above
			if (leftPlan.PTop != null && rightPlan.PTop != null) {
				Plan p2 = new Plan();
				p2.cost = leftPlan.PTop.cost + rightPlan.PTop.cost;
				p2.benefit = leftPlan.PTop.benefit + rightPlan.PTop.benefit;
				Action a2 = new Action();
				a2.option = 2;
				a2.left = leftPlan.PTop.actions;
				a2.right = rightPlan.PTop.actions;

	        	updatePlan(pTop, p2);
	        	updatePlan(best, p2);
			}

			// Just Re-Use the Best Plans found for the left/right subtree
			if (leftPlan.Best != null && rightPlan.Best != null) {
				Plan p3 = new Plan();
				p3.cost = leftPlan.Best.cost + rightPlan.Best.cost;
				p3.benefit = leftPlan.Best.benefit + rightPlan.Best.benefit;
				Action a3 = new Action();
				a3.option = 3;
				a3.left = leftPlan.Best.actions;
				a3.right = rightPlan.Best.actions;

	        	updatePlan(best, p3);
			} else if (rightPlan.Best != null) {
				Plan p4 = new Plan();
				p4.cost = rightPlan.Best.cost;
				p4.benefit = rightPlan.Best.benefit;
				Action a4 = new Action();
				a4.option = 4;
				a4.right = rightPlan.Best.actions;

	        	updatePlan(best, p4);
			} else if (leftPlan.Best != null) {
				Plan p5 = new Plan();
				p5.cost = leftPlan.Best.cost;
				p5.benefit = leftPlan.Best.benefit;
				Action a5 = new Action();
				a5.option = 5;
				a5.left = leftPlan.Best.actions;

	        	updatePlan(best, p5);
			}

			if (node.attribute == p.attribute) {

			}

			return new Plans();
		}
	}

//	public Plan getBestPlanForPredicate(List<LevelNode> nodes, Predicate p) {
//		while (true) {
//			for (int i=0; i<nodes.size(); i++) {
//				if (i < nodes.size() - 1) {
//					if (nodes.get(i).current == nodes.get(i+1).current) {
//						RNode old = nodes.get(i).current;
//						if (checkValid(old, p.attribute, p.type, p.value)) {
//							RNode r = old.clone();
//							r.attribute = p.attribute;
//							r.type = p.type;
//							r.value = p.value;
//							replaceInTree(old, r);
//
//					        populateBucketEstimates(r);
//					        int numAccessedOld = 0;
//					        int numAcccessedNew = getNumTuplesAccessed(r);
//					        int benefit = numAccessedOld - numAcccessedNew;
//
//					        if (benefit > 0) {
//					        	// compute cost
//					        }
//
//					        // Restore
//					        replaceInTree(r, old);
//						}
//					} else {
//						RNode old = nodes.get(i).current;
//
//						// Reached a choke point
//						if (old.attribute == p.attribute) {
//
//						} else {
//							// No go - this is the end of our journey
//							// This can happen when we have multiple predicates
//						}
//					}
//				}
//			}
//		}
//	}

	public static int getDepthOfIndex(int numBlocks) {
		int k = 31 - Integer.numberOfLeadingZeros(numBlocks);
		if (numBlocks == (int)Math.pow(2, k)) {
			return k;
		} else {
			return k+1;
		}
	}
}
