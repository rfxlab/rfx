package rfx.core.stream.util;


import io.netty.handler.codec.http.QueryStringDecoder;

import java.util.List;
import java.util.Map;

import rfx.core.util.LogUtil;
import rfx.core.util.StringPool;
import rfx.core.util.StringUtil;

public class ParamUtil {


	public final static Integer getIntParam(Map<String, List<String>> params, String key) {
		try {
			if (params.containsKey(key)) {
				List<String> vals = params.get(key);
				if (vals != null) {
					int index = 0;
					if (index >= 0 && index < vals.size()) {
						return Integer.parseInt(vals.get(index));
					}
				}
			}
		}
		catch (Exception e) {
			LogUtil.e("ParamUtil.getIntParam, key : " + key, e.getMessage());
		}
		return 0;
	}

	public final static Long getLongParam(Map<String, List<String>> params, String key) {
		try {
			if (params.containsKey(key)) {
				List<String> vals = params.get(key);
				if (vals != null) {
					int index = 0;
					if (index >= 0 && index < vals.size())
						return Long.parseLong(vals.get(index));
				}
			}
		}
		catch (Exception e) {
			LogUtil.e("ParamUtil.getLongParam, key : " + key, e.getMessage());
		}
		return 0l;
	}

	public static Map<String, List<String>> getQueryMap(String query) {
		QueryStringDecoder decoder = new QueryStringDecoder("?" + query);
		return decoder.parameters();
	}

	public final static String getParam(Map<String, List<String>> params, String key, String defaultVal, int index) {
		if (params.containsKey(key)) {
			List<String> vals = params.get(key);
			if (vals != null) {
				if (index >= 0 && index < vals.size())
					return (vals.get(index));
			}
		}
		return defaultVal;
	}
	
	public final static String getParam(Map<String, List<String>> params, String key, String defaultVal) {
		return getParam(params, key, defaultVal, 0);
	}

	public final static String getParam(Map<String, List<String>> params, String key) {
		return getParam(params, key, StringPool.BLANK, 0);
	}
	
	public final static String getParam(Map<String, List<String>> params, String key, int index) {
		return getParam(params, key, StringPool.BLANK, index);
	}
	
	
	public static final String extractUserId(String cookie) {
		try {
			if (cookie.indexOf("userid=") >= 0) {
				cookie = cookie.substring(cookie.indexOf("userid=") + 7);
				String v = cookie.substring(0, 16);
				return v;
			}
		}
		catch (Throwable e) {}
		return StringPool.BLANK;
	}
	
	public static String getParam(Map<String, List<String>> params, String key, String defaultVal, String where) {
		if (params.containsKey(key)) {
			List<String> vals = params.get(key);
			if (vals != null) {
				if (!StringUtil.isEmpty(where)) {
					for (String val : vals) {
						if (val.contains(where)) {
							return val;
						}
					}
				}
				else {
					return vals.get(0);
				}

			}
		}
		return defaultVal;
	}
	
}
