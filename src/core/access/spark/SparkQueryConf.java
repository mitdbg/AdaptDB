package core.access.spark;

import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Joiner;

import core.access.Predicate;

public class SparkQueryConf {

	public final static String DATASET = "DATASET";
	public final static String PREDICATES = "PREDICATES";
	public final static String WORKERS = "WORKERS";

	private Configuration conf;

	public SparkQueryConf(Configuration conf){
		this.conf = conf;
	}

	public void setDataset(String dataset){
		conf.set(DATASET, dataset);
	}

	public String getDataset(){
		return conf.get(DATASET);
	}

	public void setPredicates(Predicate[] predicates){
		conf.set(PREDICATES, Joiner.on(",").join(predicates));
	}

	public Predicate[] getPredicates(){
		String[] tokens = conf.get(PREDICATES).split(",");
		Predicate[] predicates = new Predicate[tokens.length];
		// TODO: Do predicate parsing
		// for(int i=0; i<predicates.length; i++)
			// predicates[i] = new Predicate();
		return predicates;
	}

	public void setWorkers(int workers){
		conf.set(WORKERS, ""+workers);
	}

	public int getWorkers(){
		return Integer.parseInt(conf.get(WORKERS));
	}
}
