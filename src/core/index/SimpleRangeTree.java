package core.index;

import java.util.List;
import java.util.Random;

import core.index.key.CartilageIndexKey2;
import core.index.key.MDIndexKey;
import core.utils.RangeUtils;
import core.utils.RangeUtils.Range;
import core.utils.RangeUtils.SimpleDateRange.SimpleDate;
import core.utils.RangeUtils.StringSet;
import core.utils.SchemaUtils.TYPE;

/**
 * A simple range tree which collects the ranges of all index 
 * attributes in the build phase and constructs a range tree over that. 
 * 
 * @author alekh
 *
 */
public class SimpleRangeTree implements MDIndex {

	private TYPE[] types;
	private Range[] valueRanges;
	@SuppressWarnings("unused")
	private int bucketSize;
	
	private int numPartitions;
	private Random r;
	
	public SimpleRangeTree(int numPartitions){
		this.numPartitions = numPartitions;
	}
	
	public SimpleRangeTree clone() throws CloneNotSupportedException{
		return new SimpleRangeTree(numPartitions);
	}
	
	public void initBuild(int bucketSize) {
		this.bucketSize = bucketSize;
		this.r = new Random();
	}
	
	private void initRanges(TYPE[] types){
		this.types = types;
		valueRanges = new Range[types.length];
		for(int i=0; i<types.length; i++){
			switch(types[i]){
			case INT:		valueRanges[i] = RangeUtils.closed(Integer.MAX_VALUE, Integer.MIN_VALUE); break;
			case LONG:		valueRanges[i] = RangeUtils.closed(Long.MAX_VALUE, Long.MIN_VALUE); break;
			case FLOAT:		valueRanges[i] = RangeUtils.closed(Float.MAX_VALUE, Float.MIN_VALUE); break;
			case DATE:		valueRanges[i] = RangeUtils.closed(new SimpleDate(9999,99,99), new SimpleDate(0000,00,00)); break;
			case STRING:	valueRanges[i] = new RangeUtils.StringSet(); break;
			//case VARCHAR:	break;	// skip partitioning on varchar attribute
			default:		throw new RuntimeException("Unknown dimension type: "+types[i]);
			} 
		}
	}

	public void insert(MDIndexKey key) {
		CartilageIndexKey2 k = (CartilageIndexKey2)key;
		if(valueRanges==null)
			initRanges(k.detectTypes());
		
		for(int i=0; i<types.length; i++){
			switch(types[i]){
			case INT:		int vi = k.getIntAttribute(i);
							if(vi < (Integer)(valueRanges[i].getLow()))
								valueRanges[i].setLow(vi);
							if(vi > (Integer)(valueRanges[i].getHigh()))
								valueRanges[i].setHigh(vi);
							break;
							
			case LONG:		long vl = k.getLongAttribute(i);
							if(vl < (Long)(valueRanges[i].getLow()))
								valueRanges[i].setLow(vl);
							if(vl > (Long)(valueRanges[i].getHigh()))
								valueRanges[i].setHigh(vl);
							break;				
							
			case FLOAT:		float vf = k.getFloatAttribute(i);
							if(vf < (Float)(valueRanges[i].getLow()))
								valueRanges[i].setLow(vf);
							if(vf > (Float)(valueRanges[i].getHigh()))
								valueRanges[i].setHigh(vf);
							break;				
							
			case DATE:		SimpleDate vd = k.getDateAttribute(i);
							if(vd.compareTo((SimpleDate)valueRanges[i].getLow())<0)
								valueRanges[i].setLow(vd);
							if(vd.compareTo((SimpleDate)valueRanges[i].getHigh())>0)
								valueRanges[i].setHigh(vd);
							break;				
							
			case STRING:	if(((StringSet)valueRanges[i]).size() > 10){
								// the string attribute is perhaps a varchar
								types[i] = TYPE.VARCHAR;
								valueRanges[i] = null;
							}
							else
								valueRanges[i].setLow(k.getStringAttribute(i,20)); 
							break;
			case VARCHAR:	break; // skip partitioning on varchar attribute
			default:		throw new RuntimeException("Unknown dimension type: "+types[i]);
			}
		}
	}

	public void bulkLoad(MDIndexKey[] keys) {
		throw new UnsupportedOperationException("bulk load not yet supported");
	}

	public void initProbe() {
		
		for(int i=0;i<valueRanges.length; i++)
			System.out.println(valueRanges[i]);
		
		
		// the tree building algorithm
		
		// logn buckets = levels
		
		// Step 1: check the number of fan-out (n), assuming all heterogeneous
		// 	- if n>=2: then all attributes can be partitioned fully (one at each level)
		// 	- else: the attributes can only be partially partitioned
		
		// Step 2: 
		
	}

	public Object getBucketId(MDIndexKey key) {
		// TODO Auto-generated method stub
		return ""+r.nextInt(numPartitions);
	}

	public Bucket search(MDIndexKey key) {
		// TODO Auto-generated method stub
		return null;
	}

	public List<Bucket> range(MDIndexKey low, MDIndexKey high) {
		// TODO Auto-generated method stub
		return null;
	}

	public byte[] marshall() {
		// TODO Auto-generated method stub
		return null;
	}

	public void unmarshall(byte[] bytes) {
		// TODO Auto-generated method stub
	}
	
	
//	/**
//	 *	Get the fan-out: f = [b / 2^(l-1)] ^ {2^(l-1) / [A - 2^(l-1) + 1]}
//	 * 
//	 * @param b	- number of buckets
//	 * @param a	- number of attributes
//	 * @param l	- the level in the tree
//	 * @return
//	 */
//	static double getFanout(int b, int a, int l){
//		int x = (int)Math.pow(2, l-1);
//		return Math.pow((double)b/x, (double)x/(a-x+1));
//	}
//	
//	static double getCoverage(int b, int a, int r){
//		return 0;
//	}
//	
//	public static void main(String[] args) {
//		
//		int b = 8;		// buckets
//		int a = 6;		// attributes
//		
//		for(int l=1; l<=3; l++)
//			System.out.println("fanout = "+getFanout(b, a, l));
//		
//	}
}
