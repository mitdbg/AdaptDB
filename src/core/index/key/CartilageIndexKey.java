package core.index.key;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import com.google.common.primitives.Ints;

import core.crtlg.CartilageDataflow;
import core.data.CartilageDatum.CartilageBinaryRecord;
import core.utils.DateUtils;
import core.utils.RangeUtils.SimpleDateRange.SimpleDate;
import core.utils.SchemaUtils.TYPE;
import core.utils.TypeUtils;

public class CartilageIndexKey implements MDIndexKey{

	public static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	private CartilageBinaryRecord record;
	private int[] keyAttrIdx;
	
	public CartilageIndexKey(){
	}
	
	public CartilageIndexKey(int[] keyAttrIdx){
		this.keyAttrIdx = keyAttrIdx;
	}
	
	public void setTuple(Object tuple) {
		this.record = (CartilageBinaryRecord)tuple;
	}

	public TYPE[] detectTypes(){
		List<TYPE> types = new ArrayList<TYPE>();
		
		String[] tokens = record.toString().split("\\"+CartilageDataflow.delimiter);
		for(int i=0;i<tokens.length;i++){
			if(keyAttrIdx!=null && !Ints.contains(keyAttrIdx, i))
				continue;
			
			String t = tokens[i].trim();
			if(t.equals(""))
				continue;
			
			if(TypeUtils.isInt(t))
				types.add(TYPE.INT);
			else if(TypeUtils.isLong(t))
				types.add(TYPE.LONG);
			else if(TypeUtils.isFloat(t))
				types.add(TYPE.FLOAT);
			else if(TypeUtils.isDate(t, DateUtils.sdf))
				types.add(TYPE.DATE);
			else
				types.add(TYPE.STRING);
			
		}
		
		if(keyAttrIdx==null){
			keyAttrIdx = new int[types.size()];
			for(int i=0;i<keyAttrIdx.length;i++)
				keyAttrIdx[i] = i;
		}
		
		TYPE[] typeArr = new TYPE[types.size()];
		for(int i=0;i<types.size();i++)
			typeArr[i] = types.get(i);
		
		return typeArr;
	}
	
	public String getKeyString() {
		return new String(record.getBytes());
	}

	public String getStringAttribute(int index, int size) {
		return record.getValue(keyAttrIdx[index]);
	}

	public int getIntAttribute(int index) {
		return record.getIntValue(keyAttrIdx[index]);
	}

	public long getLongAttribute(int index) {
		return record.getLongValue(keyAttrIdx[index]);
	}

	public float getFloatAttribute(int index) {
		return record.getFloatValue(keyAttrIdx[index]);
	}
	
	public double getDoubleAttribute(int index) {
		return record.getDoubleValue(keyAttrIdx[index]);
	}
	
	public SimpleDate getDateAttribute(int index){
//		try {
			//return sdf.parse(record.getValue(keyAttrIdx[index]));
			return null;
//		} catch (ParseException e) {
//			throw new RuntimeException("could not parse date");
//		}
	}
	
	
	public static void main(String[] args) {
		
		String s = "2147483648";
		System.out.println(Integer.parseInt(s));
		
	}
	
}
