package core.adapt;

import java.util.ArrayList;
import java.util.List;


public class AccessStats {
	List<Query> window;
		
	public AccessStats() {
		window = new ArrayList<Query>();
	}
	
	public void collect(Query q){
		
	}
}
