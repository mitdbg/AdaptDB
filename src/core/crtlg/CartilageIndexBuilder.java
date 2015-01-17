package core.crtlg;

import core.conf.CartilageConf;
import core.data.CartilageDatum;
import core.index.MDIndex;
import core.index.MDIndexKey;
import core.udf.CartilageUDF;

public class CartilageIndexBuilder extends CartilageUDF{

	private MDIndex mdIndex;
	private MDIndexKey mdIndexKey;
	
	public CartilageIndexBuilder(MDIndex mdIndex, MDIndexKey mdIndexKey){
		this.mdIndex = mdIndex;
		this.mdIndexKey = mdIndexKey;
	}
	
	public void initialize(CartilageConf arg0) {
		currentDatum = 0;
	}

	public boolean hasNext() {
		if(currentDatum < datum.size())
			return true;
		else{
			currentDatum = 0;
			return false;
		}
	}

	public CartilageDatum getNext() {
		mdIndex.insert(mdIndexKey.extract(datum.get(currentDatum)));	// get the next tuple from upstream operator
		currentDatum++;
		return null;
	}

	public void finalize() {
	}
}
