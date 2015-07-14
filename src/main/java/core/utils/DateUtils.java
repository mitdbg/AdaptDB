package core.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtils {

	public static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	
	public static Date parseDate(String dateString){
		try {
			return sdf.parse(dateString);
		} catch (ParseException e) {
			throw new RuntimeException("could not parse date: "+dateString);
		}
	}
	
	public static String dateToString(Date date){
		return sdf.format(date);
	}
}
