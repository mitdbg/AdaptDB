package core.access.spark.join;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.google.common.collect.ArrayListMultimap;

import core.access.AccessMethod;
import core.access.spark.HPInput;
import core.access.spark.SparkQueryConf;
import core.index.robusttree.RobustTreeHs;

public class HPJoinInput extends HPInput{

	private String joinInput;
	private Integer joinKey;
	@SuppressWarnings("unused")
	private Integer rid;
	
	// the index number of this input amongst the join relations
	private static int idx = 0;
	
	// hard coded .. must be configured
	int joinReplica = 0;
	String hadoopNamenode = "hdfs://istc2.csail.mit.edu:9000";
	
	
	public HPJoinInput(Configuration conf){
		joinInput = conf.get("JOIN_INPUT"+(idx+1));
		String joinCond = conf.get("JOIN_CONDITION");
		String tokens[] = joinCond.split("=");
		rid = Integer.parseInt(tokens[idx].split("\\.")[0]);
		joinKey = Integer.parseInt(tokens[idx].split("\\.")[1]);
		idx++;	// increment the idx for the next instantiation of HPJoinInput
		
		conf.set(FileInputFormat.INPUT_DIR, hadoopNamenode + joinInput+"/"+joinReplica);		
	}
	
	public void initialize(List<FileStatus> files, SparkQueryConf queryConf){
		AccessMethod am = new AccessMethod();
		queryConf.setWorkingDir(joinInput);
		queryConf.setReplicaId(joinReplica);
		am.init(queryConf);
		super.initialize(files, am);
	}
	
	
	
	// utility methods
	
	public RobustTreeHs getIndex(){
		return am.getIndex();
	}
	
	public int getJoinKey(){
		return joinKey;
	}
	
	public ArrayListMultimap<Integer,FileStatus> getPartitionIdFileMap(){
		return partitionIdFileMap;
	}
}
