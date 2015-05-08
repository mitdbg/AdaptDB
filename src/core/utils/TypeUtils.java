package core.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Comparator;

import core.utils.RangeUtils.SimpleDateRange.SimpleDate;
import core.utils.SchemaUtils.TYPE;

public class TypeUtils {

	public static boolean isInt(String s){
		try{
			Integer.parseInt(s);
			return true;
		} catch(NumberFormatException e){
			return false;
		}
	}

	public static boolean isLong(String s){
		try{
			Long.parseLong(s);
			return true;
		} catch(NumberFormatException e){
			return false;
		}
	}

	public static boolean isFloat(String s){
		try{
			Float.parseFloat(s);
			return true;
		} catch(NumberFormatException e){
			return false;
		}
	}

	public static boolean isDouble(String s){
		try{
			Double.parseDouble(s);
			return true;
		} catch(NumberFormatException e){
			return false;
		}
	}

	// dateFormat e.g. "yyyy-MM-dd"
	public static boolean isDate(String s, SimpleDateFormat sdf){
		try {
			sdf.parse(s);
			return true;
		} catch (ParseException e) {
			return false;
		}
	}

	public static int compareTo(Object x, Object y, TYPE t) {
		switch(t) {
		case INT:
			if ((Integer)x > (Integer)y) return 1;
			else if ((Integer)x < (Integer)y) return -1;
			else return 0;
		case FLOAT:
			if ((Float)x > (Float)y) return 1;
			else if ((Float)x < (Float)y) return -1;
			else return 0;
		case LONG:
			if ((Long)x > (Long)y) return 1;
			else if ((Long)x < (Long)y) return -1;
			else return 0;
		case DATE:
			return ((SimpleDate)x).compareTo((SimpleDate)y);
		case STRING:
		case VARCHAR:
			if (x.hashCode() > y.hashCode()) return 1;
			else if (x.hashCode() < y.hashCode()) return -1;
			else return 0;
		default:
			System.err.println("Unknown TYPE in compareTo");
			return 0;
		}
	}

	// TODO: Make this compatible with the one in TypeUtils
	public static Comparator<Object> getComparatorForType(TYPE type) {
		switch(type){
		case INT:
			return new Comparator<Object> (){
				public int compare(Object o1, Object o2) {
					return ((Integer)o1).compareTo((Integer)o2);
				}
			};
		case LONG:
			return new Comparator<Object> (){
				public int compare(Object o1, Object o2) {
					return ((Long)o1).compareTo((Long)o2);
				}
			};
		case FLOAT:
			return new Comparator<Object> (){
				public int compare(Object o1, Object o2) {
					return ((Float)o1).compareTo((Float)o2);
				}
			};
		case DATE:
			return new Comparator<Object> (){
				public int compare(Object o1, Object o2) {
					return ((SimpleDate)o1).compareTo((SimpleDate)o2);
				}
			};
		case STRING:
			return new Comparator<Object> (){
				public int compare(Object o1, Object o2) {
					return ((String)o1).compareTo((String)o2);
				}
			};
		case VARCHAR:
			throw new RuntimeException("sorting over varchar is not supported"); // skip partitioning on varchar attribute
		default:
			throw new RuntimeException("Unknown dimension type: "+ type);
		}
	}

	public static String serializeValue(Object value, TYPE t) {
		return value.toString();
	}

	public static Object deserializeValue(TYPE t, String token) {
		switch(t) {
		case INT:
			return Integer.parseInt(token);
		case LONG:
			return Long.parseLong(token);
		case FLOAT:
			return Float.parseFloat(token);
		case DATE:
			byte[] bytes = token.getBytes();
			int off = 0;
			int year = 1000*(bytes[off]-'0') + 100*(bytes[off+1]-'0') + 10*(bytes[off+2]-'0') + (bytes[off+3]-'0');
			int month = 10*(bytes[off+5]-'0') + (bytes[off+6]-'0');
			int day = 10*(bytes[off+8]-'0') + (bytes[off+9]-'0');

			return new SimpleDate(year, month, day);
		case BOOLEAN:
			return Boolean.parseBoolean(token);
		case STRING:
			return token;
		case VARCHAR:
			return token;
		default:
			return token;
		}
	}

	public static Object deltaLess(Object value, TYPE t) {
		switch(t) {
		case INT:
			return (Integer)value - 1;
		case LONG:
			return (Long)value - 1;
		case FLOAT:
			return (Float)value - 0.001;
		case DATE:
			SimpleDate d = (SimpleDate)value;
			return d.oneDayLess();
		case BOOLEAN:
			return false;
		case STRING:
			System.err.println("ERR: Called deltaLess on string");
			return value;
		case VARCHAR:
			System.err.println("ERR: Called deltaLess on string");
			return value;
		default:
			System.err.println("ERR: Called deltaLess on unknown type");
			return value;
		}
	}
}
