package server.http.util;

import io.netty.handler.codec.http.HttpHeaders.Names;
import io.netty.handler.codec.http.HttpResponseStatus;

import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.HttpServerResponse;

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
				res.headers().set(Names.LOCATION, redirect);
				res.headers().set(Names.CONNECTION, "Close");
				res.end();
			}
		} else {
			res.setStatusCode(HttpResponseStatus.MOVED_PERMANENTLY.code());
			res.headers().set(Names.LOCATION, "http://eclick.vn");
			res.headers().set(Names.CONNECTION, "Close");
			res.end();
		}
	}
		
	public static ClickBeaconData decodeClickUri(String uri){
		try {
			String[] toks = uri.split("://");
			String [] toks2 = toks[0].split("/");
			
			String redirectUrl = StringUtil.toString(toks2[toks2.length-1] , "://" , toks[1]);
			String beacon = toks2[toks2.length-2];
			
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