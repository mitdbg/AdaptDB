package core.adapt.opt;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import core.access.Predicate;
import core.access.Query;
import core.access.Query.FilterQuery;
import core.index.MDIndex.Bucket;
import core.index.key.CartilageIndexKeySet;
import core.index.robusttree.RNode;
import core.index.robusttree.RobustTreeHs;
import core.utils.ConfUtils;
import core.utils.HDFSUtils;
import core.utils.Pair;
import core.utils.SchemaUtils.TYPE;

/**
 * Finds the best index tree for a given workload
 * @author anil
 *
 */
public class WorkloadAnalyser {
	public class Candidate {
		public int attribute;
	    public TYPE type;
	    public Object value;
	}
	
	List<Query> queryWindow = new ArrayList<Query>();
	List<Candidate> candidates = new LinkedList<Candidate>();
	RobustTreeHs rt;
	
	/**
	 * 
	 * @param hdfsHomeDir
	 * @param depth depth of index tree
	 */
	public WorkloadAnalyser(ConfUtils conf, int depth) {
		String hadoopHome = conf.getHADOOP_HOME();
		String hdfsHomeDir = conf.getHDFS_HOMEDIR();
		String pathToQueries = hdfsHomeDir + "/queries";
		String pathToSample = hdfsHomeDir + "/sample";
		
		FileSystem fs = HDFSUtils.getFS(hadoopHome + "/etc/hadoop/core-site.xml");
		try {
			if (fs.exists(new Path(pathToQueries))) {
				byte[] queryBytes = HDFSUtils.readFile(fs, pathToQueries);
				String queries = new String(queryBytes);
				Scanner sc = new Scanner(queries);
				while (sc.hasNextLine()) {
					String query = sc.nextLine();
					FilterQuery f = new FilterQuery(query);
					queryWindow.add(f);
				}
				sc.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		for (int i=0; i<queryWindow.size(); i++) {
			Query k = queryWindow.get(i);
			if (k instanceof FilterQuery) {
				FilterQuery fq = (FilterQuery) k;
				Predicate[] ps = fq.getPredicates();
				for (int j=0; j<ps.length; j++) {
					Candidate c = new Candidate();
					c.attribute = ps[j].attribute;
					c.type = ps[j].type;
					c.value = ps[j].getHelpfulCutpoint();
					candidates.add(c);
				}
			}
		}
		
		rt = new RobustTreeHs(0.01);
		byte[] sampleBytes = HDFSUtils.readFile(fs, pathToSample);
		rt.sample.unmarshall(sampleBytes);
		
		buildOptTree(rt.getRoot(), rt.sample);
	}
	
	public void buildOptTree(RNode n, CartilageIndexKeySet sample) {
		// Make node point to imaginary attribute
		n.attribute = 100;

		n.leftChild = new RNode();
		n.leftChild.bucket = new Bucket();

		n.rightChild = new RNode();
		n.rightChild.bucket = new Bucket();
        double numAccessedOld = getNumTuplesAccessed(n);
		
        double benefit = 0;
        Candidate cChosen = null;
        
		for (Candidate c: candidates) {
			n.attribute = c.attribute;
			n.type = c.type;
			n.value = c.value;
			
			Pair<CartilageIndexKeySet, CartilageIndexKeySet> halves = sample.splitAt(n.attribute, n.value);
			n.leftChild.bucket.setSample(halves.first);
			
			n.rightChild.bucket.setSample(halves.second);

			// TODO: Populate bucket estimates
	        double numAccessedNew = getNumTuplesAccessed(n);
	        double extraBenefit = numAccessedOld - numAccessedNew;
	        
	        if (extraBenefit > benefit) {
	        	benefit = extraBenefit;
	        	cChosen = c;
	        }
		}
		
		if (cChosen == null) {
			n.bucket = new Bucket();
			n.bucket.setSample(sample);
		} else {
			n.attribute = cChosen.attribute;
			n.type = cChosen.type;
			n.value = cChosen.value;

			Pair<CartilageIndexKeySet, CartilageIndexKeySet> halves = sample.splitAt(n.attribute, n.value);
			n.leftChild.bucket = null;
			n.rightChild.bucket = null;
			buildOptTree(n.leftChild, halves.first);
			buildOptTree(n.rightChild, halves.second);
		}
	}
	
	public double getNumTuplesAccessed(RNode changed) {
		double numTuples = 0;
		for (int i=queryWindow.size() - 1; i >= 0; i--) {
			Query q = queryWindow.get(i);
			numTuples += Optimizer.getNumTuplesAccessed(changed, q, false);
		}

		return numTuples;
	}
}
