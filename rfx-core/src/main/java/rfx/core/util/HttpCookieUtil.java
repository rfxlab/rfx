package rfx.core.util;

import static io.netty.handler.codec.http.HttpHeaders.Names.SET_COOKIE;
import static io.netty.handler.codec.http.HttpHeaders.Names.USER_AGENT;

import java.net.URLDecoder;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.HttpServerResponse;

import io.netty.handler.codec.http.Cookie;
import io.netty.handler.codec.http.CookieDecoder;
import io.netty.handler.codec.http.DefaultCookie;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.ServerCookieEncoder;
import io.netty.handler.codec.http.HttpHeaders.Names;



public class HttpCookieUtil {
	
	public final static String P3P = "CP='CURa ADMa DEVa PSAo PSDo OUR BUS UNI PUR INT DEM STA PRE IND PHY ONL COM NAV OTC NOI DSP COR IDC'";
	
	public final static String DEFAULT_PATH = "/";
	public final static String COOKIE_SEPARATOR = ".";
	
	public final static long COOKIE_AGE_10_YEARS = 630720000;
	public final static long COOKIE_AGE_2_YEARS = 63072000;
	public final static long COOKIE_AGE_1_HOUR = 3600; // One hour
	public final static long COOKIE_AGE_2_HOURS = 7200; // 2 hours
	public final static long COOKIE_AGE_3_HOURS = 10800; // 3 hours	
	public final static long COOKIE_AGE_1_DAY = 86400; // One day
	public final static long COOKIE_AGE_3_DAYS = 259200; // 3 days
	public final static long COOKIE_AGE_1_WEEK = 604800; // One week

	/*
	 * fosp_rand format 3b10f37d26bae61d.1330937373.4.1331004249.1330998456.2
	 * FOSP_aiod. first time visit, number visit, current time, last time visit,
	 * locationId
	 */
	public static String newFospRand(String rand_aid, int rand_locationId) {
		int rand_numbervisit = 1;
		int rand_numberidvisit = 1;
		long unixTime = System.currentTimeMillis() / 1000L;
		int rand_currenttime = (int) unixTime;
		int rand_firsttime = rand_currenttime;
		int rand_lasttimevisit = rand_currenttime;
		return HttpCookieUtil.generateFospRand(rand_aid, rand_firsttime,
				rand_numbervisit, rand_currenttime, rand_lasttimevisit,rand_numberidvisit,
				rand_locationId);
	}

	public static String generateFospRand(String rand_aid, int rand_firsttime,int rand_numbervisit, int rand_currenttime, 
			int rand_lasttimevisit,int rand_numberidvisit,	int rand_locationId) {
		StringBuilder cookieString = new StringBuilder();
		cookieString.append(rand_aid);
		cookieString.append(COOKIE_SEPARATOR);
		cookieString.append(rand_firsttime);
		cookieString.append(COOKIE_SEPARATOR);
		cookieString.append(rand_numbervisit);
		cookieString.append(COOKIE_SEPARATOR);
		cookieString.append(rand_currenttime);
		cookieString.append(COOKIE_SEPARATOR);
		cookieString.append(rand_lasttimevisit);
		cookieString.append(COOKIE_SEPARATOR);
		cookieString.append(rand_numberidvisit);
		cookieString.append(COOKIE_SEPARATOR);
		cookieString.append(rand_locationId);		
		return SecurityUtil.encryptBeaconValue(cookieString.toString());
	}

	public static void setAnomyousCookie(Cookie cookie,	FullHttpResponse response) {
		response.headers().add(SET_COOKIE, ServerCookieEncoder.encode(cookie));
	}

	

	public static boolean isValidVisitorId(String vid) {
		if (vid.length() == 16 && vid.matches("^[0-9A-Fa-f]+$")) {
			return true;
		} else {
			return false;
		}
	}

	public static Cookie createNewHttpOnlyCookie(String name, String value,
			String domain, String path) {
		Cookie cookie = new DefaultCookie(name, value);
		cookie.setDomain(domain);
		cookie.setPath(path);
		cookie.setHttpOnly(true);
		return cookie;
	}

	public static Cookie createCookie(String name, String value, String domain,
			String path) {
		Cookie cookie = new DefaultCookie(name, value);
		cookie.setDomain(domain);
		cookie.setPath(path);
		return cookie;
	}

	public static String generateFospAID(HttpRequest request) {		
		String userAgent = request.headers().get(USER_AGENT);
		String logDetails = request.headers().get(io.netty.handler.codec.http.HttpHeaders.Names.HOST);
		String result = SecurityUtil.sha1(userAgent + logDetails + System.currentTimeMillis());
		return result.substring(0, 16);
	}
	// create ID for browser not support cookies and localStorage
	public static String generateVisitorIdByIp(String ipAddress, String browser, String os) {		
		String result = SecurityUtil.sha1(ipAddress + browser + os);
		return result.substring(0, 16);
	}

		public static void addExpireTime2YearsForCookie(Cookie cookie) {
		cookie.setMaxAge(COOKIE_AGE_2_YEARS);
	}
	
	public static void addExpireTimeForCookie(Cookie cookie, long maxAge) {
		cookie.setMaxAge(maxAge);
	}
	

	public static String getStrParam(String[] params, int position) {
		String result = "";
		try {
			result = params[position];
		} catch (Exception e) {
			result = "";
		}
		return result;
	}

	public static Set<Cookie> getCookies(HttpServerRequest req) {
		try {
			String cookieString = req.headers().get(Names.COOKIE);		
			if (cookieString != null) {
				cookieString = URLDecoder.decode(cookieString, StringPool.UTF_8);				
				return CookieDecoder.decode(cookieString);
			}
		} catch (Exception e) {			
			e.printStackTrace();
		}
		return new HashSet<Cookie>(0);
	}
	
	public static Map<String,Cookie> getCookieMap(HttpServerRequest req) {
		try {
			String cookieString = req.headers().get(Names.COOKIE);		
			if (cookieString != null) {
				cookieString = URLDecoder.decode(cookieString, StringPool.UTF_8);				
				Set<Cookie> cookies = CookieDecoder.decode(cookieString);
				Map<String,Cookie> map = new HashMap<String,Cookie>(cookies.size());
				for (Cookie cookie : cookies) {
					map.put(cookie.getName(), cookie);
				}
				return map;
			}
		} catch (Exception e) {			
			e.printStackTrace();
		}
		return new HashMap<String,Cookie>(0);
	}
	
	public static void setCookie(HttpServerResponse res, String name, String value, String domain, String path) {
		setCookie(res, name, value, domain, path, COOKIE_AGE_3_HOURS);
	}
	
	public static void setCookie(HttpServerResponse res, String name, String value, String domain, String path, long maxAge) {
		try {			
			Cookie cookie = new DefaultCookie(name, value);
			cookie.setDomain(domain);
			cookie.setPath(path);			
			cookie.setMaxAge(maxAge);
			res.headers().add(SET_COOKIE,ServerCookieEncoder.encode(cookie));
		} catch (Exception e) {			
			e.printStackTrace();
		}
	}
}
