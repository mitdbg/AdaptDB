package core.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;

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
}
