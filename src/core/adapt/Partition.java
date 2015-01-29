package core.adapt;



public abstract class Partition {

	
//	public static enum State {ORIG,NEW,MODIFIED};
//	private Map<Attribute,Range> attributeRanges;	// the list of ranges which characterize this partition
//	private State state;	
	
	
	public void create(){
	}
	
	public boolean loadNext(){
		return false;	// load the next block of this partition 
	}
	
	public abstract PartitionIterator iterate();
	
	public abstract void store();
	
	public abstract void drop();
	
}
