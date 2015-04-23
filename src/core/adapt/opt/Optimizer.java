package core.adapt.opt;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import core.adapt.Predicate;
import core.adapt.Query;
import core.adapt.Query.FilterQuery;
import core.index.key.CartilageIndexKey;
import core.index.key.CartilageIndexKeySet;
import core.index.robusttree.RNode;
import core.index.robusttree.RobustTreeHs;
import core.utils.Pair;
import core.utils.SchemaUtils.TYPE;
import core.utils.TypeUtils;

/**
 *
 * @author anil
 *
 */

public class Optimizer {
	RobustTreeHs rt;
	int rtDepth;

	String dataset;

	static final int BLOCK_SIZE = 64 * 1024;
	static final float DISK_MULTIPLIER = 4;
	static final float NETWORK_MULTIPLIER = 7;

	List<Query> queryWindow = new ArrayList<Query>();

	public static class Plan {
		public Action actions; // Tree of actions
		public float cost;
		public float benefit;

		public Plan() {
			cost = -1;
		}
	}

	public static class Action {
		public int option; // Says which if I used to create this plan
		public Action left;
		public Action right;
	}

	public static class Plans {
		boolean fullAccess;
		Plan PTop; // TODO: Think if this is needed at all
		Plan Best;

		public Plans() {
			this.fullAccess = false;
			this.PTop = null;
			this.Best = null;
		}
	}

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

			this.rt = new RobustTreeHs(numBlocks, 0.01);

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
			Plan best = getBestPlan(ps);
			this.getPartitionSplits(best, numWorkers);
			this.queryWindow.add(q);
		} else {
			System.err.println("Unimplemented query - Unable to build plan");
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
				this.updatePlan(plan, option);
			}
		}

		return plan;
	}

	public void getPartitionSplits (Plan best, int numWorkers) {

	}

	/**
	 * Checks if it is valid to partition on attrId with value val at node
	 * @param node
	 * @param attrId
	 * @param t
	 * @param val
	 * @return
	 */
	public boolean checkValid(RNode node, int attrId, TYPE t, Object val) {
		while (node.parent != null) {
			if (node.parent.attribute == attrId) {
				int ret = TypeUtils.compareTo(node.parent.value, val, t);
				if (node.parent.leftChild == node && ret <= 0) {
					return false;
				} else if (ret >= 0) {
					return false;
				}
			}

			node = node.parent;
		}

		return true;
	}

	/**
	 * Puts the new estimated number of tuples in each bucket after change
	 * @param changed
	 */
	public void populateBucketEstimates(RNode changed) {
		CartilageIndexKeySet collector = null;
		float numTuples = 0;
		int numSamples = 0;

		LinkedList<RNode> stack = new LinkedList<RNode>();
		stack.add(changed);

		while (stack.size() > 0) {
			RNode n = stack.removeLast();
			if (n.bucket != null) {
				CartilageIndexKeySet bucketSample = n.bucket.getSample();
				if (collector == null) {
					collector = new CartilageIndexKeySet();
					collector.setTypes(bucketSample.getTypes());
				}

				numSamples += bucketSample.getValues().size();
				numTuples += n.bucket.getNumTuples();
				collector.addValues(bucketSample.getValues());
			} else {
				stack.add(n.rightChild);
				stack.add(n.leftChild);
			}
		}

		populateBucketEstimates(changed, collector, numTuples/numSamples);
	}


	public void populateBucketEstimates(RNode n, CartilageIndexKeySet sample, float scaleFactor) {
		if (n.bucket != null) {
			n.bucket.estimatedTuples = sample.size() * scaleFactor;
		} else {
			// By sorting we avoid memory allocation
			// Will most probably be faster
			sample.sort(n.attribute);
			Pair<CartilageIndexKeySet, CartilageIndexKeySet> halves = sample.splitAt(n.attribute, n.value);
			populateBucketEstimates(n.leftChild, halves.first, scaleFactor);
			populateBucketEstimates(n.rightChild, halves.second, scaleFactor);
		}
	}

	/**
	 * Gives the number of tuples accessed
	 * @param changed
	 * @return
	 */
	public int getNumTuplesAccessed(RNode changed) {
		// First traverse to parent to see if query accesses node
		// If yes, find the number of tuples accessed.
		int numTuples = 0;

		for (Query q: queryWindow) {
			Predicate[] ps = ((FilterQuery)q).getPredicates();

			RNode node = changed;
			boolean accessed = true;
			while (node.parent != null) {
				for (Predicate p: ps) {
					if (p.attribute == node.parent.attribute) {
						if (node.parent.leftChild == node) {
		        			switch (p.predtype) {
		                	case EQ:
		        			case GEQ:
		                		if (TypeUtils.compareTo(p.value, node.parent.value, node.parent.type) > 0) accessed = false;
		                		break;
		                	case GT:
		                		if (TypeUtils.compareTo(p.value, node.parent.value, node.parent.type) >= 0) accessed = false;
		                		break;
							default:
								break;
		        			}
						} else {
		        			switch (p.predtype) {
		                	case EQ:
		        			case LEQ:
		                		if (TypeUtils.compareTo(p.value, node.parent.value, node.parent.type) <= 0) accessed = false;
		                		break;
		                	case LT:
		                		if (TypeUtils.compareTo(p.value, node.parent.value, node.parent.type) < 0) accessed = false;
		                		break;
							default:
								break;
		        			}
						}
					}

					if (!accessed) break;
				}

				if (!accessed) break;
			}

			if (accessed) {
				List<RNode> nodesAccessed = changed.search(ps);
				int tCount = 0;
				for (RNode n: nodesAccessed) {
					tCount += n.bucket.getNumTuples();
				}

				// TODO: Possible exponential distribution
				numTuples += tCount;
			}
		}

		return numTuples;
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
		} else if (dest.benefit / dest.cost < source.benefit / source.cost) {
			copy = true;
		}

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
			Plans ret = new Plans();

			Plans leftPlan = getBestPlanForSubtree(node.rightChild, p);
			Plans rightPlan = getBestPlanForSubtree(node.rightChild, p);

			// replace attribute by one in the predicate
			if (leftPlan.fullAccess && rightPlan.fullAccess) {
				ret.fullAccess = true;

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
				int c = checkEquals(node.value, p.value, node.type);
				if (c != 0) {
					assert (c < 0 && leftPlan.PTop == null) || (c > 0 && rightPlan.PTop == null);
					if (c < 0 && rightPlan.PTop != null) {
						Plan p6 = new Plan();
						p6.cost = rightPlan.PTop.cost;
						p6.benefit = rightPlan.PTop.benefit;
						Action a6 = new Action();
						a6.option = 6;
						a6.right = rightPlan.PTop.actions;

			        	updatePlan(pTop, p6);
			        	updatePlan(best, p6);
					}
					if (c > 0 && leftPlan.PTop != null) {
						Plan p6 = new Plan();
						p6.cost = leftPlan.PTop.cost;
						p6.benefit = leftPlan.PTop.benefit;
						Action a6 = new Action();
						a6.option = 6;
						a6.left = leftPlan.PTop.actions;

						updatePlan(pTop, p6);
			        	updatePlan(best, p6);
					}
				}
			}

			if (pTop.cost != -1) ret.PTop = pTop;
			if (best.cost != -1) ret.Best = best;

			return ret;
		}
	}

	public static int checkEquals(Object v1, Object v2, TYPE type) {
		switch (type) {
			case INT:
				return ((Integer)v1).compareTo((Integer)v2);
			case FLOAT:
				return ((Float)v1).compareTo((Float)v2);
			case LONG:
				return ((Long)v1).compareTo((Long)v2);
			case BOOLEAN:
				return ((Boolean)v1).compareTo((Boolean)v2);
			case STRING:
				return ((String)v1).compareTo((String)v2);
			case DATE:
				return ((Date)v1).compareTo((Date)v2);
			case VARCHAR:
				return 0; // TODO: What is this ?
		}

		return 0;
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
