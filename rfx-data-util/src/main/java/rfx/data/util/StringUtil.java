package rfx.data.util;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.Formatter;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jsoup.Jsoup;



import com.google.gson.Gson;

public class StringUtil {
	static final String TAG = "storm.StringUtil";

	public static Long Dot2LongIP(String dottedIP) {
	    String[] addrArray = dottedIP.split("\\.");
	    long num = 0;
	    for (int i=0;i<addrArray.length;i++) {
	        int power = 3-i;
	        num += ((Integer.parseInt(addrArray[i]) % 256) * Math.pow(256,power));
	    }
	    return num;
	}

	public static String html2text(String html) {
		return Jsoup.parse(html).text();
	}

	public static String toString(Object ...args) {
		StringBuilder s = new StringBuilder();
		for (Object arg : args) {
			if(arg != null){
				s.append(arg);
			}
		}
		return s.toString();
	}

	public static boolean isStringContain(String text, String words) {
		// Pattern p = Pattern.compile("YOUR_REGEX", Pattern.CASE_INSENSITIVE |
		// Pattern.UNICODE_CASE);
		// p.m
		return text.matches("(?i)(.*)" + words + "(.*)");
	}

	public static String decodeUrlUTF8(String s){
		try {
			return java.net.URLDecoder.decode( safeString(s),StringPool.UTF_8);
		} catch (Exception e) {
			if(e instanceof java.lang.IllegalArgumentException){
				int l = s.lastIndexOf("%");
				if(l == s.length()-1){
					s = s.substring(0, l);
					try {
						return java.net.URLDecoder.decode( safeString(s),StringPool.UTF_8);
					} catch (Exception e1) {
						e1.printStackTrace();
					}
				}
			}
		}
		return s;
	}

	public static String encodeUrlUTF8(String s){
		try {
			return java.net.URLEncoder.encode( safeString(s),StringPool.UTF_8);
		} catch (Throwable e) {
			e.printStackTrace();
		}
		return StringPool.BLANK;
	}

	private static String byteArray2Hex(final byte[] hash) {
		Formatter formatter = new Formatter();
		for (byte b : hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}

	public static String SHA1(byte[] convertme) {
		MessageDigest md;
		try {
			md = MessageDigest.getInstance("SHA-1");
			return byteArray2Hex(md.digest(convertme));
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return "";
	}



	public static String safeSubString(String s, int maxLength){
		if(maxLength <= 1){
			return s;
		}
		if(isEmpty(s)){
			return StringPool.BLANK;
		}
		s = s.trim();
		return s.length() <= 255 ? s : s.substring(0,maxLength - 1);
	}


	public static String replace(String value, String returnNewLine,
			String newLine) {
		if (value == null)
			return "";
		return value.replace(returnNewLine, newLine);
	}

	public static boolean isNullOrEmpty(String s) {
		if (s == null || "".equals(s)) {
			return true;
		}
		return false;
	}

	public static boolean isEmpty(String s) {
		if (s == null) {
			return true;
		}
		return s.trim().isEmpty();
	}

	public static boolean isEmpty(Object s) {
		if (s == null) {
			return true;
		}
		return s.toString().isEmpty();
	}

	public static boolean isNotEmpty(String s) {
		return ! isEmpty(s);
	}


	public static boolean isValidUrl(String url) {
		if(isNotEmpty(url)){
			return url.startsWith("http") && url.contains("://");
		}
		return false;
	}
	
	public static String join(String[] array, String joinStr){		
		return join(array, joinStr);
	}
	
	public static String join(Object[] array, String joinStr){
		StringBuilder s = new StringBuilder();
		int l = array.length, n = l -1;
		for (int i=0; i<l;i++) {
			if(i<n){
				s.append(array[i]).append(joinStr);
			} else {
				s.append(array[i]);
			}
		}
		return s.toString();
	}

	public static String convertObjectToJson(Object arg) {
		if(arg != null){
			return new Gson().toJson(arg);
		}
		return StringPool.BLANK;
	}

	public static String joinString(String[] toks, String delimiter){
		StringBuilder s = new StringBuilder();
		for (String tok : toks) {
			s.append(tok).append(delimiter);
		}
		return s.toString();
	}

	public static String join(String delimiter, Object... array){
		StringBuilder s = new StringBuilder();
		int l = array.length, lastIndex = l -1;
		for (int i=0; i<l;i++) {
			if(i<lastIndex){
				s.append(array[i]).append(delimiter);
			} else {
				s.append(array[i]);
			}
		}
		return s.toString();
	}
	
	public static String joinFromCollection(String delimiter, Collection<Object> collection){
		return join(delimiter, collection.toArray());
	}
	
	public static String joinFromList(String delimiter, List<String> list){
		return join(delimiter, list.toArray());
	}

	public static int safeParseInt(String s){
		return safeParseInt(s, 0);
	}

	public static int safeParseInt(Object s){
		if (s == null) {
			return 0;
		}
		return safeParseInt(s.toString(), 0);
	}

	public static int safeParseInt(Object s, int defaultVal){
		if(isEmpty(s)){
			return defaultVal;
		}
		int n = defaultVal;
		try {
			n = Integer.parseInt(s.toString());
		} catch (Throwable e) {	}
		return n;
	}
	
	public static double round(double value, int places) {
	    if (places < 0) throw new IllegalArgumentException();

	    long factor = (long) Math.pow(10, places);
	    value = value * factor;
	    long tmp = Math.round(value);
	    return (double) tmp / factor;
	}


	public static double safeParseDouble(String s){
		if(isEmpty(s)){
			return 0;
		}
		double n = 0;
		try {
			n = Double.parseDouble(s.trim());
		} catch (Throwable e) {	}
		return round(n,2);
	}

	public static String safeString(Object s, String defaultVal) {
		if (isEmpty(s)) {
			return defaultVal;
		}
		return String.valueOf(s);
	}

	public static String safeString(String s) {
		return safeString(s,StringPool.BLANK);
	}

	public static String safeString(String s, int validMaxLendth, String defaultVal) {
		if (s == null) {
			return defaultVal;
		}
		if(s.length() > validMaxLendth){
			return defaultVal;
		}
		return s;
	}

	public static String safeSplitAndGet(String s,String delimiter, int i){
		try {
			String[] toks = safeString(s).split(delimiter);
			if( i < toks.length ){
				if( ! isEmpty(toks[i])){
					return toks[i];
				}
			}
		} catch (Throwable e) {	}
		return StringPool.BLANK;
	}

	public static long safeParseLong(Object s){
		if(s == null){
			return 0;
		}
		long n = 0;
		try {
			n = Long.parseLong(s.toString().trim());
		} catch (Throwable e) {}
		return n;
	}

	public static long safeParseLong(String s){
		return safeParseLong(s,0);
	}

	public static long safeParseLong(Object s, long defaultVal){
		long n = defaultVal;
		if(s == null){
			return n;
		}		
		try {
			n = Long.parseLong(s.toString().trim());
		} catch (Throwable e) {	}
		return n;
	}
	
	public static float safeParseFloat(String s){
		return safeParseFloat(s,0);
	}

	public static float safeParseFloat(Object s, float defaultVal){
		float n = defaultVal;
		if(s == null){
			return n;
		}		
		try {
			n = Float.parseFloat(s.toString().trim());
		} catch (Throwable e) {	}
		return n;
	}

	public static String toString(ByteBuffer buffer){
		/// Create a StringBuffer so that we can convert the bytes to a String
		StringBuffer response = new StringBuffer();

		// Create a CharSet that knows how to encode and decode standard text (UTF-8)
		Charset charset = Charset.forName("UTF-8");

		// Decode the buffer to a String using the CharSet and append it to our buffer
		response.append( charset.decode( buffer ) );
		buffer.flip();
		return response.toString();
	}


	public static String toString(Map<String, String> map, String kvDelimeter, String eDelimeter){
		StringBuilder s = new StringBuilder();
		Set<String> keys = map.keySet();
		for (String k : keys) {
			String v = map.get(k);
			if(s.length() == 0){
				s.append(k).append(kvDelimeter).append(v);
			} else {
				s.append(eDelimeter).append(k).append(kvDelimeter).append(v);
			}
		}
		return s.toString();
	}

}
