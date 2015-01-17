package core.crtlg;

import core.conf.CartilageConf;
import core.data.CartilageDatum;
import core.index.MDIndex;
import core.index.MDIndexKey;
import core.udf.partition.CartilageLogicalPartitioner;

public class CartilagePartitionMapper extends CartilageLogicalPartitioner{

	private MDIndex mdIndex;
	private MDIndexKey mdIndexKey;
	
	private boolean returnFlag;
	
	public CartilagePartitionMapper(MDIndex mdIndex, MDIndexKey mdIndexKey){
		this.mdIndex = mdIndex;
		this.mdIndexKey = mdIndexKey;
	}
	
	protected void initializePartitioner(CartilageConf arg0) {
		returnFlag = true;
	}

	protected CartilageDatum getNextPartition(CartilageDatum currentDatum) {
		if(returnFlag){
			returnFlag = false;
			CartilageDatum returnPartition = currentDatum;
			returnPartition.currentLabel = mdIndex.getBucketId(mdIndexKey.extract(currentDatum));
			return returnPartition;
		}
		else{
			returnFlag = true;
			return null;
		}
	}
	
	protected void finalizePartitioner() {
	}
}
