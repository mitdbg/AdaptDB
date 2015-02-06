package core.crtlg;

import core.conf.CartilageConf;
import core.data.CartilageDatum;
import core.index.MDIndex;
import core.index.key.MDIndexKey;
import core.udf.CartilageUDF;

public class CartilageIndexBuilder extends CartilageUDF{

	private MDIndex mdIndex;
	private MDIndexKey mdIndexKey;
	
	public CartilageIndexBuilder(MDIndex mdIndex, MDIndexKey mdIndexKey){
		this.mdIndex = mdIndex;
		this.mdIndexKey = mdIndexKey;
	}
	
	public void initialize(CartilageConf arg0) {
		currentDatum = -1;
	}

	public boolean hasNext() {
		currentDatum++;
		
		if(currentDatum < datum.size())
			return true;
		else{
			currentDatum = -1;
			return false;
		}
	}

	public CartilageDatum getNext() {
		CartilageDatum d = datum.get(currentDatum);
		mdIndexKey.setTuple(d);	// get the next tuple from upstream operator
		mdIndex.insert(mdIndexKey);	
		return d;
	}

	public void finalize() {
	}
}
