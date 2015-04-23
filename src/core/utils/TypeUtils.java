package core.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

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
			if ((Integer)x > (Integer)y) return 1;
			else if ((Integer)x < (Integer)y) return -1;
			else return 0;
		case LONG:
			if ((Integer)x > (Integer)y) return 1;
			else if ((Integer)x < (Integer)y) return -1;
			else return 0;
		case DATE:
			return ((Date)x).compareTo((Date)y);
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
}
