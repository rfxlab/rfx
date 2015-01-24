package server.http.test;

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import rfx.core.util.StringPool;


public class HttpClientCaller {	
		
	public static final Charset CHARSET_UTF8 = Charset.forName(StringPool.UTF_8);
	static int DEFAULT_TIMEOUT = 10000;//10 seconds
	public static final String USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.143 Safari/537.36";
	public static final String MOBILE_USER_AGENT = "Mozilla/5.0 (Linux; U; Android 2.2; en-us; DROID2 GLOBAL Build/S273) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1";
	
	final static int MAX_SIZE = 20;
	static ConcurrentMap<Integer, HttpClient> httpClientPool = new ConcurrentHashMap<>(MAX_SIZE);
	
	public static final HttpClient getThreadSafeClient() throws Exception {
		int slot = (int)(Math.random() * (MAX_SIZE + 1));		
	    return getThreadSafeClient(slot);
	}
	
	public static final HttpClient getThreadSafeClient(int slot) throws Exception {		
		HttpClient httpClient = httpClientPool.get(slot);
		if(httpClient == null){
			httpClient = HttpClients.createMinimal();
		    httpClientPool.put(slot, httpClient);
		}
	    return httpClient;
	}	
		
	public static String executeGet(final URL url){
		HttpResponse response = null;
		HttpClient httpClient = null;
		//System.out.println("executeGet:" + url);
		try {
			HttpGet httpget = new HttpGet(url.toURI());			
			httpget.setHeader("User-Agent", USER_AGENT);
			httpget.setHeader("Accept-Charset", "utf-8");
			httpget.setHeader("Accept", "text/html,application/xhtml+xml");
			httpget.setHeader("Cache-Control", "max-age=3, must-revalidate, private");		
			httpClient = getThreadSafeClient();
			response = httpClient.execute(httpget);	
			
			int code = response.getStatusLine().getStatusCode();
			if (code == 200) {				
				HttpEntity entity = response.getEntity();
				if (entity != null) {
					return EntityUtils.toString(entity, CHARSET_UTF8);
				}
			} else if(code == 404) {
				return "404";
			} else {
				return "500";
			}
		}  catch (Throwable e) {
			return "444";
		} finally {
			response = null;
		}
		return "";
	}	
	public static String executeGet(final String url){
		try {
			return executeGet(new URL(url));
		} catch (MalformedURLException e) {			
			e.printStackTrace();
		}
		return "";
	}	
	
	public static void main(String[] args) {
		String rs = HttpClientCaller.executeGet("http://vnexpress.net/");
		System.out.println(rs);
	}	
	
}
