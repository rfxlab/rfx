package server.http.util;




import static io.netty.handler.codec.http.HttpHeaderNames.*;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import rfx.core.util.LogUtil;
import rfx.core.util.StringUtil;
import server.http.model.ClickBeaconData;

public class RedirectUtil {

	
	public static void redirect(String uri, HttpServerRequest req){		
		ClickBeaconData cd = RedirectUtil.decodeClickUri(uri);		
		HttpServerResponse res = req.response();
		if (cd != null) {
			String redirect = cd.getRedirectUrl();			
			if (redirect.startsWith("http")) {
				res.setStatusCode(HttpResponseStatus.MOVED_PERMANENTLY.code());
				res.headers().set(LOCATION, redirect);
				res.headers().set(CONNECTION, "Close");
				res.end();
			}
		} else {
			System.err.println("ClickBeaconData is NULL, redirect to default URL");
			res.setStatusCode(HttpResponseStatus.MOVED_PERMANENTLY.code());
			res.headers().set(LOCATION, "http://eclick.vn");
			res.headers().set(CONNECTION, "Close");
			res.end();
		}
	}
		
	public static ClickBeaconData decodeClickUri(String uri){
		try {
			String[] toks = uri.split("://");
			String [] toks2 = toks[0].split("/");
			
			String redirectUrl = StringUtil.toString(toks2[toks2.length-1] , "://" , toks[1]);
			System.out.println("["+redirectUrl+"]");
			String beacon = toks2[toks2.length-2];
			
			System.out.println("redirectUrl "+redirectUrl);
			
			return new ClickBeaconData(redirectUrl, beacon);
		} catch (Exception e) {
			if(e instanceof ArrayIndexOutOfBoundsException){
				//skip
			} else {
				e.printStackTrace();
				LogUtil.e("RedirectUtil.decodeClickUri", e.getMessage());	
			}			
		}
		return null;
	}
	
	
}