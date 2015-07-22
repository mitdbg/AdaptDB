package core.access.spark.join;

import org.apache.hadoop.io.LongWritable;
import org.apache.spark.api.java.JavaPairRDD;

import core.access.spark.SparkQuery;
import core.utils.ConfUtils;

public class SparkJoinQuery extends SparkQuery{

	public SparkJoinQuery(ConfUtils config) {
		super(config);
	}
	
	public JavaPairRDD<LongWritable, SparkJoinRecordReader.JoinTuplePair> createJoinRDD(boolean assignBuckets, String hdfsPath1, int rid1, int joinAttribute1, String hdfsPath2, int rid2, int joinAttribute2){

		ctx.hadoopConfiguration().setBoolean("ASSIGN_BUCKETS", assignBuckets);
		ctx.hadoopConfiguration().set("JOIN_INPUT1", hdfsPath1);
		ctx.hadoopConfiguration().set("JOIN_INPUT2", hdfsPath2);
		ctx.hadoopConfiguration().set("JOIN_CONDITION", rid1+"."+joinAttribute1+"="+rid2+"."+joinAttribute2);
		ctx.hadoopConfiguration().set("HADOOP_NAMENODE", cfg.getHADOOP_NAMENODE());

		queryConf.setHadoopHome(cfg.getHADOOP_HOME());
		queryConf.setZookeeperHosts(cfg.getZOOKEEPER_HOSTS());
		queryConf.setHDFSReplicationFactor(cfg.getHDFS_REPLICATION_FACTOR());

		System.out.println(hdfsPath1 +";"+  hdfsPath2);

		return ctx.newAPIHadoopFile(
				hdfsPath1 + ";" + hdfsPath2,
				SparkJoinInputFormat.class,
				LongWritable.class,
				SparkJoinRecordReader.JoinTuplePair.class,
				ctx.hadoopConfiguration());
	}

//	public static abstract class JoinFlatMapFunction<K,V> implements FlatMapFunction<Iterator<Tuple2<LongWritable,IteratorRecord>>, Tuple2<V,V>> {
//		private static final long serialVersionUID = 1L;
//		private SparkHashJoin<K,V> hj;
//		protected int rid1, rid2;
//		protected int joinAttribute1, joinAttribute2;
//		public JoinFlatMapFunction(int rid1, int joinAttribute1, int rid2, int joinAttribute2){
//			this.rid1 = rid1;
//			this.rid2 = rid2;
//			this.joinAttribute1 = joinAttribute1;
//			this.joinAttribute2 = joinAttribute2;
//			hj = new SparkHashJoin<K,V>();
//		}
//		public Iterable<Tuple2<V,V>> call(Iterator<Tuple2<LongWritable, IteratorRecord>> arg0) throws Exception {
//			// perform join
//			hj.initialize(rid1);	// dataset1 is the left hand relation
//			while(arg0.hasNext()){
//				Tuple2<LongWritable, IteratorRecord> t = arg0.next();
//				int rid = (int)t._1().get();
//				hj.add(rid, getKey(rid,t._2()), getValue(rid,t._2()));
//			}
//			// return result
//			Iterable<Tuple2<V,V>> result = hj.getJoinResults();
//			hj.clear();
//			return result;
//		}
//		protected abstract K getKey(int rid, IteratorRecord r);
//		protected abstract V getValue(int rid, IteratorRecord r);
//	}
//
//	public static class StringStringJoin extends JoinFlatMapFunction<String,String>{
//		private static final long serialVersionUID = 1L;
//		public StringStringJoin(int rid1, int joinAttribute1, int rid2, int joinAttribute2) {
//			super(rid1, joinAttribute1, rid2, joinAttribute2);
//		}
//		protected String getKey(int rid, IteratorRecord r) {
//			if(rid==rid1)
//				return ""+r.getLongAttribute(joinAttribute1);
//			else if(rid==rid2)
//				return ""+r.getLongAttribute(joinAttribute2);
//			else
//				throw new RuntimeException("Unknown relation "+rid);
//		}
//		protected String getValue(int rid, IteratorRecord r) {
//			return r.getKeyString();
//		}
//	}
}
