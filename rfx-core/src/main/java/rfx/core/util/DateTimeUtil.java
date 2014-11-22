package rfx.core.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

public class DateTimeUtil {
	
	public static final int ONE_WEEK_IN_SECS = 604800;
	public static final int TWO_DAYS_IN_SECS = 172800;
	public static final int HOURS_36_IN_SECS = 129600;
	
	public static final String DATE_FORMAT_PATTERN = "yyyy-MM-dd";
	public static final String DATE_HOUR_FORMAT_PATTERN = "yyyy-MM-dd-HH";
	public static final String DATE_HOUR_MINUTE_FORMAT_PATTERN = "yyyy-MM-dd-HH-mm";
	public static final String DATE_HOUR_KEY_PATTERN = "yyyy-MM-dd HH:00:00";
	public static final String HOUR_PATTERN = "HH";
	
	static final DateFormat DATE_FORMAT = new SimpleDateFormat("dd/MM/yyyy");
	static final DateFormat DATE_FORMAT2 = new SimpleDateFormat(DATE_FORMAT_PATTERN);
	static final DateFormat DATE_NAME_FORMAT = new SimpleDateFormat("yyyy/MM/dd");
	static final DateFormat HOUR_NAME_FORMAT = new SimpleDateFormat("yyyy/MM/dd HH:00:00");
	
	static final DateFormat HOUR_NAME_FORMAT2 = new SimpleDateFormat("yyyy/MM/dd HH");
	
	//friendly format for RDBMS Database, e.g: MySQL, Oracle, PostgresSQL
	static final DateFormat DATE_TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");	
	static final DateFormat DB_HOUR_NAME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:00:00");
	static final DateFormat DATE_FORMAT_FOR_DB = new SimpleDateFormat("yyyy-MM-dd");	
	
	static final DateFormat DATEHOUR_FORMAT = new SimpleDateFormat("yyyy-MM-dd-HH");
	static final DateFormat DATEHOURMINUTE_FORMAT = new SimpleDateFormat("yyyy-MM-dd-HH-mm");

	public static String formatRawDateTime(int unixtime){
		return DATE_TIME_FORMAT.format(new Date(unixtime * 1000L));
	}
	
	public static String toString(String formatPattern, Date date){
		DateFormat dateFormat = new SimpleDateFormat(formatPattern);		
		return dateFormat.format(date);
	}

	public static String formatDate(Date d ){
		return DATE_FORMAT.format(d);
	}
	
	public static String formatDateName(Date d ){
		return DATE_FORMAT.format(d);
	}
	
	public static String formatHourName(Date d ){
		return DATE_FORMAT.format(d);
	}
	
	public static long getTimestampFromDateHour(String d){
		try {
			return HOUR_NAME_FORMAT.parse(d).getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return 0;
	}
	
	public static long getTimestampFromDateHour2(String d){
		try {
			return HOUR_NAME_FORMAT2.parse(d).getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return 0;
	}
	
	public static long getTimestampFromDate(String d){
		try {
			return DATE_NAME_FORMAT.parse(d).getTime();
		} catch (ParseException e) {
			System.err.println(d + " getTimestampFromDate " + e.getMessage());
		}
		return 0;
	}
	
	public static int getTimestampFromDate(java.sql.Date d){
		if(d != null){
			return (int) (d.getTime()/1000);
		}
		return 0;
	}
	
	public static String formatDate(Date d, String format ){
		DateFormat dateFormat = new SimpleDateFormat(format);
		return dateFormat.format(d);
	}
	
	public static String getDateStringForDb(Date d ){
		return DATE_FORMAT_FOR_DB.format(d);
	}
	
	public static String getDateHourStringForDb(Date d ){
		return DB_HOUR_NAME_FORMAT.format(d);
	}
	
	public static String getDateHourString(Date d ){
		return DATEHOUR_FORMAT.format(d);
	}
	
	public static Date parseDateStrRaw(String str){
		try {
			DateFormat DATE_TIME_FORMAT = (DateFormat) DateTimeUtil.DATE_TIME_FORMAT.clone();
			return DATE_TIME_FORMAT.parse(str);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return new Date();
	}
	
	public static Date parseDateStr(String str) throws ParseException{
		return DATE_FORMAT.parse(str);		
	}
	
	public static Date parseDateHourStr(String str) throws ParseException{
		return DATEHOUR_FORMAT.parse(str);		
	}
	
	public static Date parseDateHourMinuteStr(String str) throws ParseException{
		return DATEHOURMINUTE_FORMAT.parse(str);		
	}
	
	public static String formatDateHourMinute(Date date) {
		return DATEHOURMINUTE_FORMAT.format(date);		
	}
	
	public static int currentUnixTimestamp(){
		return (int)(System.currentTimeMillis()/1000L);
	}
	
	public static String getFormatedDateForLog(){
		return DATE_FORMAT2.format(new Date());
	}
	
	public static void main(String[] args) {
		System.out.println(DateTimeUtil.getDateHourStringForDb(new Date(1394792343 * 1000L)));
	}
	
	public static Date jsDateStringToJavaDate(String jsDateString) throws ParseException{
	    String[] arrStrDateParts = jsDateString.split(" ");
	    SimpleDateFormat sdf = new SimpleDateFormat("E MMM dd yyyy HH:mm:ss", Locale.ENGLISH);
	    sdf.setTimeZone(TimeZone.getTimeZone(arrStrDateParts[5].substring(0,6)+":"+arrStrDateParts[5].substring(6)));
	    return sdf.parse(arrStrDateParts[0]+" "+arrStrDateParts[1]+" "+arrStrDateParts[2]+" "+arrStrDateParts[3]+" "+arrStrDateParts[4]);       
	}
}
