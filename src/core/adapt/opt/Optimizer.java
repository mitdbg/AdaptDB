package core.adapt.opt;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;

import core.access.AccessMethod.PartitionSplit;
import core.access.Predicate;
import core.access.Query;
import core.access.Query.FilterQuery;
import core.access.iterator.PartitionIterator;
import core.access.iterator.PostFilterIterator;
import core.access.iterator.RepartitionIterator;
import core.index.key.CartilageIndexKeySet;
import core.index.robusttree.RNode;
import core.index.robusttree.RobustTreeHs;
import core.utils.HDFSUtils;
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

	static final int NODE_MEM_SIZE = 1024 * 1024 * 1024 * 2;
	static final int TUPLE_SIZE = 48;
	static final int NODE_TUPLE_LIMIT = NODE_MEM_SIZE / TUPLE_SIZE;

	List<Query> queryWindow = new ArrayList<Query>();

	public static class Plan {
		public Action actions; // Tree of actions
		public float cost;
		public float benefit;

		public Plan() {
			cost = 0;
			benefit = 0;
		}
	}

	public static class Action {
		public int pid; // Predicate
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

	public void loadIndex(String hadoopHome) {
		FileSystem fs = HDFSUtils.getFS(hadoopHome + "/etc/hadoop/core-site.xml");
		String pathToIndex = this.dataset + "/index";
		String pathToSample = this.dataset + "/sample";
		byte[] indexBytes = HDFSUtils.readFile(fs, pathToIndex);
		this.rt = new RobustTreeHs(0.01);
		this.rt.unmarshall(indexBytes);

		byte[] sampleBytes = HDFSUtils.readFile(fs, pathToSample);
		this.rt.loadSample(sampleBytes);
	}

	public PartitionSplit[] buildAccessPlan(final Query q, int numWorkers) {
		if (q instanceof FilterQuery) {
			FilterQuery fq = (FilterQuery) q;
			List<RNode> nodes = this.rt.getRoot().search(fq.getPredicates());
			PartitionIterator pi = new PostFilterIterator(fq);
			int[] bids = new int[nodes.size()];
			Iterator<RNode> it = nodes.iterator();
		    for (int i=0; i<bids.length; i++) {
		    	bids[i] = it.next().bucket.getBucketId();
		    }

			PartitionSplit psplit = new PartitionSplit(bids, pi);
			PartitionSplit[] ps = new PartitionSplit[1];
			ps[0] = psplit;
			return ps;
		} else {
			System.err.println("Unimplemented query - Unable to build plan");
			return null;
		}
	}

	public PartitionSplit[] buildPlan(final Query q, int numWorkers) {
		if (q instanceof FilterQuery) {
			FilterQuery fq = (FilterQuery) q;
			Plan best = getBestPlan(fq.getPredicates());
			PartitionSplit[] psplits = this.getPartitionSplits(best, fq, numWorkers);
			this.queryWindow.add(q);
			this.updateIndex(best, fq.getPredicates());
			return psplits;
		} else {
			System.err.println("Unimplemented query - Unable to build plan");
			return null;
		}
	}

	public Plan getBestPlan(Predicate[] ps) {
		//TODO: Multiple predicates seem to complicate the simple idea we had; think more :-/
		Plan plan = null;
		for (int i=0; i<ps.length; i++) {
			Plan option = getBestPlanForPredicate(ps, i);
			if (plan == null) {
				plan = option;
			} else {
				this.updatePlan(plan, option);
			}
		}

		return plan;
	}

	public void updateIndex(Plan best, Predicate[] ps) {
		this.applyActions(this.rt.getRoot(), best.actions, ps);
	}

	public void applyActions(RNode n, Action a, Predicate[] ps) {
		if (a.left != null) {
			this.applyActions(n.leftChild, a.left, ps);
		}

		if (a.right != null) {
			this.applyActions(n.rightChild, a.right, ps);
		}

		switch (a.option) {
		case 0:
			break;
		case 1:
			Predicate p = ps[a.pid];
			RNode r = n.clone();
			r.attribute = p.attribute;
			r.type = p.type;
			r.value = p.value;
			replaceInTree(n, r);
			break;
		case 2:
			Predicate p2 = ps[a.pid];
			assert p2.attribute == n.leftChild.attribute;
			assert p2.attribute == n.rightChild.attribute;
			RNode n2 = n.clone();
			RNode right = n.rightChild.clone();
			replaceInTree(n.leftChild, n2);
			replaceInTree(n, right);
			replaceInTree(right.rightChild, n);
			break;
		case 3:
			assert a.right == null || a.left == null;
			if (a.left == null) {
				RNode t = n.rightChild.clone();
				replaceInTree(n, t);
				replaceInTree(t.rightChild, n);
			} else {
				RNode t = n.leftChild.clone();
				replaceInTree(n, t);
				replaceInTree(t.leftChild, n);
			}
			break;
		case 4:
			break;
		case 5:
			break;
		}
	}

	public PartitionSplit[] getPartitionSplits (Plan best, FilterQuery fq, int numWorkers) {
		List<PartitionSplit> lps = new ArrayList<PartitionSplit>();
		final int[] modifyingOptions = new int[]{1};
		Action acTree = best.actions;

		LinkedList<RNode> nodeStack = new LinkedList<RNode>();
		nodeStack.add(this.rt.getRoot());

		LinkedList<Action> actionStack = new LinkedList<Action>();
		actionStack.add(acTree);

		List<Integer> unmodifiedBuckets = new ArrayList<Integer>();

		Predicate[] ps = fq.getPredicates();
		while (nodeStack.size() > 0) {
			RNode n = nodeStack.removeLast();
			Action a = actionStack.removeLast();

			boolean isModifying = false;
			for (int t: modifyingOptions){
		        if (t == a.option) {
		        	List<RNode> bs = n.search(ps);
		        	int[] bucketIds = new int[bs.size()];
		        	for (int i=0; i<bucketIds.length; i++) {
		        		bucketIds[i] = bs.get(i).bucket.getBucketId();
		        	}

		        	Predicate p = ps[a.pid];
		        	RNode r = n.clone();
					r.attribute = p.attribute;
					r.type = p.type;
					r.value = p.value;
					replaceInTree(n, r);

					// Give new bucket ids to all nodes below this
					updateBucketIds(r);

		        	PartitionIterator pi = new RepartitionIterator(fq, r);
		        	PartitionSplit psplit = new PartitionSplit(bucketIds, pi);
		        	lps.add(psplit);
		        	isModifying = true;
		        	break;
		        }
		    }


			if (!isModifying) {
				if (a.right != null) {
					actionStack.add(a.right);
					nodeStack.add(n.rightChild);
				}

				if (a.left != null) {
					actionStack.add(a.left);
					nodeStack.add(n.leftChild);
				}

				if (n.bucket != null) {
					unmodifiedBuckets.add(n.bucket.getBucketId());
				}
			}
		}

		if (unmodifiedBuckets.size() > 0) {
			PartitionIterator pi = new PostFilterIterator(fq);
			int[] bids = new int[unmodifiedBuckets.size()];
			Iterator<Integer> it = unmodifiedBuckets.iterator();
		    for (int i=0; i<bids.length; i++) {
		    	bids[i] = it.next();
		    }
			PartitionSplit psplit = new PartitionSplit(bids, pi);
			lps.add(psplit);
		}

		PartitionSplit[] splits =  lps.toArray(new PartitionSplit[lps.size()]);
		return splits;
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
	public float getNumTuplesAccessed(RNode changed, boolean real) {
		// First traverse to parent to see if query accesses node
		// If yes, find the number of tuples accessed.
		float numTuples = 0;

		for (Query q: queryWindow) {
			// TODO: Possible exp distribution
			numTuples += getNumTuplesAccessed(changed, q, real);
		}

		return numTuples;
	}

	public float getNumTuplesAccessed(RNode changed, Query q, boolean real) {
		// First traverse to parent to see if query accesses node
		// If yes, find the number of tuples accessed.
		float numTuples = 0;
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
			float tCount = 0;
			for (RNode n: nodesAccessed) {
				if (real) {
					tCount += n.bucket.getNumTuples();
				} else {
					tCount += n.bucket.estimatedTuples;
				}
			}

			numTuples += tCount;
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

        r.leftChild = old.leftChild;
        r.rightChild = old.rightChild;
        r.parent = old.parent;
	}

	public Plan getBestPlanForPredicate(Predicate[] ps, int i) {
		RNode root = rt.getRoot();
		Plans plans = getBestPlanForSubtree(root, ps, i);
		return plans.Best;
	}

	public void updateBucketIds(RNode r) {
		LinkedList<RNode> stack = new LinkedList<RNode>();
		stack.add(r);

		while (stack.size() > 0) {
			RNode n = stack.removeLast();
			if (n.bucket != null) {
				n.bucket.updateId();
			} else {
				stack.add(n.rightChild);
				stack.add(n.leftChild);
			}
		}
	}

	// Update the dest with source if source is a better plan
	public void updatePlan(Plan dest, Plan source) {
		boolean copy = false;
		if (source.cost == 0) {

		} else if (dest.benefit == 0) {
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

	public Plans getBestPlanForSubtree(RNode node, Predicate[] ps, int pid) {
		// Option Index
		// 0 => Just Filter
		// 1 => Replace
		// 2 => Swap down X
		// 3 =>
		// 5 => Reuse the best plan of subtrees

		if (node.bucket != null) {
			// Leaf
			Plans pl = new Plans();
			pl.fullAccess = true;
			Plan p1 = new Plan();
			p1.cost = 0;
			p1.benefit = 0;
			Action a1 = new Action();
			a1.option = 0;
			p1.actions = a1;
			pl.Best = p1;
			return pl;
		} else {
			Predicate p = ps[pid];
			Plan pTop = new Plan();
			Plan best = new Plan();
			Plans ret = new Plans();

			// Check if both sides are accessed
        	boolean goLeft = true;
        	boolean goRight = true;
        	for (int i = 0; i < ps.length; i++) {
        		Predicate pd = ps[i];
        		if (pd.attribute == node.attribute) {
        			switch (p.predtype) {
	                	case GEQ:
	                		if (TypeUtils.compareTo(pd.value, node.value, node.type) > 0) goLeft = false;
	                		break;
	                	case LEQ:
	                		if (TypeUtils.compareTo(pd.value, node.value, node.type) <= 0) goRight = false;
	                		break;
	                	case GT:
	                		if (TypeUtils.compareTo(pd.value, node.value, node.type) >= 0) goLeft = false;
	                		break;
	                	case LT:
	                		if (TypeUtils.compareTo(pd.value, node.value, node.type) < 0) goRight = false;
	                		break;
	                	case EQ:
	                		if (TypeUtils.compareTo(pd.value, node.value, node.type) <= 0) goRight = false;
	                		else goLeft = false;
	                		break;
                	}
        		}
        	}

			Plans leftPlan;
			if (goLeft) {
				leftPlan = getBestPlanForSubtree(node.rightChild, ps, pid);
			} else {
				leftPlan = new Plans();
				leftPlan.Best = null;
				leftPlan.PTop = null;
				leftPlan.fullAccess = false;
			}

			Plans rightPlan;
			if (goRight) {
				rightPlan = getBestPlanForSubtree(node.rightChild, ps, pid);
			} else {
				rightPlan = new Plans();
				rightPlan.Best = null;
				rightPlan.PTop = null;
				rightPlan.fullAccess = false;
			}


			// replace attribute by one in the predicate
			if (leftPlan.fullAccess && rightPlan.fullAccess) {
				ret.fullAccess = true;

				// If we traverse to root and see that there is no node with cutoff point less than
				// that of predicate, we can do this
				if (checkValid(node, p.attribute, p.type, p.value)) {
			        float numAccessedOld = getNumTuplesAccessed(node, true);

					RNode r = node.clone();
					r.attribute = p.attribute;
					r.type = p.type;
					r.value = p.value;
					replaceInTree(node, r);

			        populateBucketEstimates(r);
			        float numAcccessedNew = getNumTuplesAccessed(r, false);
			        float benefit = numAccessedOld - numAcccessedNew;

			        if (benefit > 0) {
			        	// TODO: Better cost model ?
			        	float cost = this.computeCost(r); // Note that buckets haven't changed
			        	Plan p1 = new Plan();
			        	p1.cost = cost;
			        	p1.benefit = benefit;
			        	Action a1 = new Action();
			        	a1.pid = pid;
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
				a2.pid = pid;
				a2.option = 2;
				a2.left = leftPlan.PTop.actions;
				a2.right = rightPlan.PTop.actions;

	        	updatePlan(pTop, p2);
	        	updatePlan(best, p2);
			}

			if (node.attribute == p.attribute) {
				int c = TypeUtils.compareTo(node.value, p.value, node.type);
				if (c != 0) {
					assert (c < 0 && leftPlan.PTop == null) || (c > 0 && rightPlan.PTop == null);
					if (c < 0 && rightPlan.PTop != null) {
						Plan p6 = new Plan();
						p6.cost = rightPlan.PTop.cost;
						p6.benefit = rightPlan.PTop.benefit;
						Action a6 = new Action();
						a6.pid = pid;
						a6.option = 3;
						a6.right = rightPlan.PTop.actions;

			        	updatePlan(pTop, p6);
			        	updatePlan(best, p6);
					}
					if (c > 0 && leftPlan.PTop != null) {
						Plan p6 = new Plan();
						p6.cost = leftPlan.PTop.cost;
						p6.benefit = leftPlan.PTop.benefit;
						Action a6 = new Action();
						a6.pid = pid;
						a6.option = 3;
						a6.left = leftPlan.PTop.actions;

						updatePlan(pTop, p6);
			        	updatePlan(best, p6);
					}
				}
			}

			// Just Re-Use the Best Plans found for the left/right subtree
			if (leftPlan.Best != null && rightPlan.Best != null) {
				Plan p3 = new Plan();
				p3.cost = leftPlan.Best.cost + rightPlan.Best.cost;
				p3.benefit = leftPlan.Best.benefit + rightPlan.Best.benefit;
				Action a3 = new Action();
				a3.pid = pid;
				a3.option = 5;
				a3.left = leftPlan.Best.actions;
				a3.right = rightPlan.Best.actions;

	        	updatePlan(best, p3);
			} else if (rightPlan.Best != null) {
				Plan p4 = new Plan();
				p4.cost = rightPlan.Best.cost;
				p4.benefit = rightPlan.Best.benefit;
				Action a4 = new Action();
				a4.pid = pid;
				a4.option = 5;
				a4.right = rightPlan.Best.actions;

	        	updatePlan(best, p4);
			} else if (leftPlan.Best != null) {
				Plan p5 = new Plan();
				p5.cost = leftPlan.Best.cost;
				p5.benefit = leftPlan.Best.benefit;
				Action a5 = new Action();
				a5.pid = pid;
				a5.option = 5;
				a5.left = leftPlan.Best.actions;

	        	updatePlan(best, p5);
			}

			if (pTop.cost != -1) ret.PTop = pTop;
			if (best.cost != -1) ret.Best = best;

			return ret;
		}
	}

	public float computeCost(RNode r) {
		int numTuples = r.numTuplesInSubtree();
		if (numTuples > NODE_TUPLE_LIMIT) {
			return (DISK_MULTIPLIER + NETWORK_MULTIPLIER) * numTuples;
		} else {
			return DISK_MULTIPLIER * numTuples;
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
