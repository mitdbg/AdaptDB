package core.adapt.split;

import java.util.List;

import core.adapt.partition.Partition;
import core.adapt.split.SplitIterator.CrackSplitIterator;
import core.adapt.split.SplitIterator.ScanSplitIterator;


/**
 * This class represents a set of blocks that get processed at a given compute node.
 * 
 * Note that in our world, each split contains all leaf-blocks of a given sub-tree.
 * This also means that a split always created based on the query.
 * 
 * @author alekh
 *
 */
public class Split {

	private List<Partition> splitPartitions;
	private boolean favourablyPartitioned;	// flag indicating whether the split is suitable partitioned for the given query.		
	
	public Split(List<Partition> splitPartitions, boolean favourablyPartitioned){
		this.splitPartitions = splitPartitions;
		this.favourablyPartitioned = favourablyPartitioned;
	}
	
	public List<Partition> getSplitPartitions(){
		return this.splitPartitions;
	}
	
	public boolean isFavourablyPartitioned(){
		return this.favourablyPartitioned;
	}
	
	public SplitIterator getIterator(){			
		return favourablyPartitioned ? 
				new ScanSplitIterator(splitPartitions) : 
					new CrackSplitIterator(splitPartitions); 			
	}
}
