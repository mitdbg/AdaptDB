package core.index.key;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.google.common.primitives.Ints;

import core.crtlg.CartilageDataflow;
import core.utils.DateUtils;
import core.utils.RangeUtils.SimpleDateRange.SimpleDate;
import core.utils.SchemaUtils.TYPE;
import core.utils.TypeUtils;

public class CartilageIndexKey2 implements MDIndexKey, Cloneable{

	private SimpleDate dummyDate = new SimpleDate(0,0,0);
	
	public static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	//private CartilageBinaryRecord record;
	protected byte[] bytes;
	protected int offset, length;
	
	protected int numAttrs;
	public TYPE[] types;
	protected int[] attributeOffsets;
	
	protected char delimiter;
	protected int[] keyAttrIdx;
	
	public CartilageIndexKey2(char delimiter){
		this.delimiter = delimiter;
	}
	
	public CartilageIndexKey2(char delimiter, int[] keyAttrIdx){
		this.delimiter = delimiter;
		this.keyAttrIdx = keyAttrIdx;
	}
	
	public CartilageIndexKey2 clone() throws CloneNotSupportedException {
		CartilageIndexKey2 k = (CartilageIndexKey2) super.clone();
		k.dummyDate = new SimpleDate(0,0,0);
        return k;
	}
	
	public void setKeys(int[] keyAttrIdx){
		this.keyAttrIdx = keyAttrIdx;
	}
	
	public void setBytes(byte[] bytes, int[] offsets) {
		this.bytes = bytes;
		this.offset = 0;
		this.length = bytes.length;
		if(this.types==null){
			this.types = detectTypes(true);
			attributeOffsets = new int[numAttrs];
		}
		
		if(offsets==null){
			int previous = 0;
			int attrIdx = 0;
			for (int i=offset; i<length; i++){
		    	if(bytes[i]==delimiter){
		    		attributeOffsets[attrIdx++] = previous;
		    		previous = i+1;
		    	}
		    }
			if(attrIdx < attributeOffsets.length)
				attributeOffsets[attrIdx] = previous;
		}
		else
			this.attributeOffsets = offsets;
	}
	
	public void setBytes(byte[] bytes) {
		this.bytes = bytes;
		this.offset = 0;
		this.length = bytes.length;
		if(this.types==null){
			this.types = detectTypes(true);
			attributeOffsets = new int[numAttrs];
		}
		
		int previous = 0;
		int attrIdx = 0;
		for (int i=offset; i<length; i++){
	    	if(bytes[i]==delimiter){
	    		attributeOffsets[attrIdx++] = previous;
	    		previous = i+1;
	    	}
	    }
		if(attrIdx < attributeOffsets.length)
			attributeOffsets[attrIdx] = previous;
	}
	
	public void setBytes(byte[] bytes, int offset, int length, int[] offsets) {
		this.bytes = bytes;
		this.offset = offset;
		this.length = length;
		if(this.types==null){
			this.types = detectTypes(true);
			attributeOffsets = new int[numAttrs];
		}
		
		if(offsets==null){
			int previous = offset;
			int attrIdx = 0;
			for (int i=offset; i<offset+length; i++){
		    	if(bytes[i]==delimiter){
		    		attributeOffsets[attrIdx++] = previous;
		    		previous = i+1;
		    	}
		    }
			if(attrIdx < attributeOffsets.length)
				attributeOffsets[attrIdx] = previous;
		}
		else
			this.attributeOffsets = offsets;
	}
	
	public void setBytes(byte[] bytes, int offset, int length) {
		this.bytes = bytes;
		this.offset = offset;
		this.length = length;
		if(this.types==null){
			this.types = detectTypes(true);
			attributeOffsets = new int[numAttrs];
		}
		
		int previous = offset;
		int attrIdx = 0;
		for (int i=offset; i<offset+length; i++){
	    	if(bytes[i]==delimiter){
	    		attributeOffsets[attrIdx++] = previous;
	    		previous = i+1;
	    	}
	    }
		if(attrIdx < attributeOffsets.length)
			attributeOffsets[attrIdx] = previous;
	}
	
	/**
	 * Extract the types of the relevant attributes (which need to be used as keys)
	 * 
	 * @return
	 */
	public TYPE[] detectTypes(boolean skipNonKey){
		List<TYPE> types = new ArrayList<TYPE>();
		
		numAttrs = 0;
		String[] tokens = new String(bytes,offset,length).split("\\"+CartilageDataflow.delimiter);
		for(int i=0;i<tokens.length;i++){
			String t = tokens[i].trim();
			if(t.equals(""))
				continue;
			
			numAttrs++;
			
			if(skipNonKey && keyAttrIdx!=null && !Ints.contains(keyAttrIdx, i))
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
		return new String(bytes,offset,length);
	}

	public String getStringAttribute(int index, int maxSize) {
		index = keyAttrIdx[index];
		int off = attributeOffsets[index];
		int strSize;
		if(index < attributeOffsets.length-1)
			strSize = attributeOffsets[index+1] - off - 1;
		else
			strSize = offset+length - off;
		return strSize > maxSize ? "" : new String(bytes, off, strSize);
		//return new String(bytes, off, strSize);
	}

	public int getIntAttribute(int index) {
		index = keyAttrIdx[index];
		int off = attributeOffsets[index];
		int len;
		if(index < attributeOffsets.length-1)
			len = attributeOffsets[index+1] - off - 1;
		else
			len = offset+length - off;
		
	    // Check for a sign.
	    int num  = 0;
	    int sign = -1;
	    final char ch  = (char)bytes[off];
	    if (ch == '-')
	        sign = 1;
	    else
	        num = '0' - ch;

	    // Build the number.
	    int i = off+1;
	    while ( i < off+len )
	        num = num*10 + '0' - (char)bytes[i++];

	    return sign * num;
	}

	public long getLongAttribute(int index) {
		index = keyAttrIdx[index];
		int off = attributeOffsets[index];
		int len;
		if(index < attributeOffsets.length-1)
			len = attributeOffsets[index+1] - off - 1;
		else
			len = offset+length - off;
		
	    // Check for a sign.
	    long num  = 0;
	    int sign = -1;
	    final char ch  = (char)bytes[off];
	    if (ch == '-')
	        sign = 1;
	    else
	        num = '0' - ch;

	    // Build the number.
	    int i = off+1;
	    while ( i < off+len )
	        num = num*10 + '0' - (char)bytes[i++];

	    return sign * num;
	}

	public float getFloatAttribute(int index) {
		index = keyAttrIdx[index];
		int off = attributeOffsets[index];
		int len;
		if(index < attributeOffsets.length-1)
			len = attributeOffsets[index+1] - 1;
		else
			len = offset+length;
		
		float ret = 0f;         // return value
		int part = 0;          // the current part (int, float and sci parts of the number)
		boolean neg = false;      // true if part is a negative number
	 
		// sign
		if ((char)bytes[off] == '-') { 
			neg = true; 
			off++; 
		}
	 
		// integer part
		while (off < len && (char)bytes[off] != '.')	
			part = part*10 + ((char)bytes[off++] - '0');
		ret = neg ? (float)(part*-1) : (float)part;
	 
		// float part
		if (off < len) {	
			off++;
			int mul = 1;
			part = 0;
			while (off < len) {	
				part = part*10 + ((char)bytes[off++] - '0'); 
				mul*=10;
			}
			ret = neg ? ret - (float)part / (float)mul : ret + (float)part / (float)mul;
		}
	 	
		return ret;
	}
	
	public double getDoubleAttribute(int index) {
		index = keyAttrIdx[index];
		int off = attributeOffsets[index];
		int len;
		if(index < attributeOffsets.length-1)
			len = attributeOffsets[index+1] - 1;
		else
			len = offset+length;
		
		double ret = 0d;         // return value
		int part = 0;          // the current part (int, float and sci parts of the number)
		boolean neg = false;      // true if part is a negative number
	 
		// sign
		if ((char)bytes[off] == '-') { 
			neg = true; 
			off++; 
		}
	 
		// integer part
		while (off < len && (char)bytes[off] != '.')	
			part = part*10 + ((char)bytes[off++] - '0');
		ret = neg ? (float)(part*-1) : (float)part;
	 
		// float part
		if (off < len) {	
			off++;
			int mul = 1;
			part = 0;
			while (off < len) {	
				part = part*10 + ((char)bytes[off++] - '0'); 
				mul*=10;
			}
			ret = neg ? ret - (float)part / (float)mul : ret + (float)part / (float)mul;
		}
	 	
		return ret;
	}
	
	public Date getGenericDateAttribute(int index, String format){
		index = keyAttrIdx[index];
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		try {
			return sdf.parse(getStringAttribute(index, 100));
		} catch (ParseException e) {
			throw new RuntimeException("could not parse date");
		}
	}
	
	public SimpleDate getDateAttribute(int index){
		index = keyAttrIdx[index];
		// parse date assuming the format: "yyyy-MM-dd"
		int off = attributeOffsets[index];
		int year = 1000*(bytes[off]-'0') + 100*(bytes[off+1]-'0') + 10*(bytes[off+2]-'0') + (bytes[off+3]-'0');
		int month = 10*(bytes[off+5]-'0') + (bytes[off+6]-'0');
		int day = 10*(bytes[off+8]-'0') + (bytes[off+9]-'0');
		
		dummyDate.setYear(year);
		dummyDate.setMonth(month);
		dummyDate.setDay(day);
		
		return dummyDate;
	}
	
	
	/**
	 * Assumes that the boolean data is represented as a single character in the ascii file.
	 * 
	 * @param index
	 * @return
	 */
	public boolean getBooleanAttribute(int index) {
		index = keyAttrIdx[index];
		int off = attributeOffsets[index];
		
		if(bytes[off]=='1' || bytes[off]=='t')
			return true;
		else if(bytes[off]=='0' || bytes[off]=='f')
			return false;
		else
			throw new RuntimeException("Cannot parse the boolean attribute: "+ bytes[off]);
	}
	
	public static void main(String[] args) {
		
		String s = "2147483648";
		System.out.println(Integer.parseInt(s));
		
	}

	@Override
	public void setTuple(Object tuple) {
		// TODO Auto-generated method stub
		
	}
	
}
