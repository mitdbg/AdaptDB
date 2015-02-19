package core.adapt.partition.iterator;

import core.adapt.partition.Partition;
import core.index.key.CartilageIndexKey2;

public class PartitionCrackIterator extends PartitionIterator<CartilageIndexKey2>{

	public PartitionCrackIterator(Partition partition) {
	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public CartilageIndexKey2 next() {
		// TODO Auto-generated method stub
		return null;
	}

//	private void crackInTwo(int posL, int posH, int med){
//		internal_int_t x1 = posL, x2 = posH;
//		while (x1 <= x2) {					// the element at x1=x2 was not inspected in the original cracking algorithm --(BUG 1)
//			if(countComp() && c[x1] < med)
//				x1++;
//			else {
//				while (x2 >= x1 && (countComp() && c[x2] >= med))	// the element at x1=x2 was not inspected in the original cracking algorithm --(BUG 1)
//													// we should first check the positions (x2 >= x1) and then check the values (c[x2] >= med) --(BUG 1.1)
//					x2--;
//
//				if(x1 < x2){				// this was not checked in the original cracking algorithm --(BUG 2)
//					exchange(c, x1,x2);
//					x1++;
//					x2--;
//				}
//			}
//		}
//
//		// check if all elements have been inspected
//		if(x1 < x2)
//			printError("Not all elements were inspected!");
//
//		x1--;
//
//		//debug(INFO, "Pivot=""%lld""",x1);
//
//		return x1;
//
//	}
//	
	
}
