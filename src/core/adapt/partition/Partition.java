package core.adapt.partition;

public abstract class Partition {
	
	public static enum State {ORIG,NEW,MODIFIED};
	State state;	
	
	public boolean load(){
		return false;	// load the physical block for this partition 
	}
	
	public abstract void store();
	
	public abstract void drop();
	
}
