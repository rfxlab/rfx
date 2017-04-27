package rfx.core.util;


import java.net.InetSocketAddress;

import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;

/**
 * Util for getting param from org.vertx.java.core.http.HttpServerRequest
 * 
 * @author trieu
 *
 */
public class HttpRequestUtil {
	
	static final String unknown = "unknown" ;
	public static String getRemoteIP(HttpServerRequest request) {
		String ipAddress = request.headers().get("X-Forwarded-For");		
		if ( ! StringUtil.isNullOrEmpty(ipAddress) && ! unknown.equalsIgnoreCase(ipAddress)) {			
			//LogUtil.dumpToFileIpLog(ipAddress);
			String[] toks = ipAddress.split(",");
			int len = toks.length;
			if(len > 1){
				ipAddress = toks[len-1];
			} else {				
				return ipAddress;
			}
		} else {		
			ipAddress = getIpAsString(request.remoteAddress());
		}		
		return ipAddress;
	}
	
	public static String getIpAsString(SocketAddress address) {
		try {
			if(address instanceof InetSocketAddress){
				return ((InetSocketAddress)address).getAddress().getHostAddress();
			}
			return address.toString().split("/")[1].split(":")[0];
		} catch (Throwable e) {
			e.printStackTrace();
		}
		return "0.0.0.0";
	}
	
	public static String getParamValue(String name, MultiMap params) {
		return getParamValue(name, params, StringPool.BLANK);
	}
	
	public static int getParamIntValue(String name, MultiMap params) {
		return StringUtil.safeParseInt(getParamValue(name, params, StringPool.BLANK));
	}
	
	public static long getParamLongValue(String name, MultiMap params) {
		return StringUtil.safeParseLong(getParamValue(name, params, StringPool.BLANK));
	}
	
	public static double getParamDoubleValue(String name, MultiMap params) {
		return StringUtil.safeParseDouble(getParamValue(name, params, StringPool.BLANK));
	}
	
	public static boolean getParamBooleanValue(String name, MultiMap params) {
		return Boolean.parseBoolean(getParamValue(name, params, StringPool.BLANK));
	}

	public static String getParamValue(String name, MultiMap params, String defaultVal) {
		String val = params.get(name);
		return StringUtil.isEmpty(val)? defaultVal : val;
	}
	

}
