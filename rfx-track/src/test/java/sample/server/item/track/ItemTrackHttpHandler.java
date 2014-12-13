package sample.server.item.track;

import org.vertx.java.core.http.HttpServerRequest;

import server.http.handler.BaseHttpHandler;
import server.http.util.RedirectUtil;
import server.http.util.ResponseUtil;

public class ItemTrackHttpHandler extends BaseHttpHandler {
	
	
	private static final String PING = "ping";
	static final String logItemTracking = "tk";
	static final String logUserActivity = "u";
	static final String redirectClickPrefix = "r/";

	@Override
	public boolean handle(HttpServerRequest req) {
		String uri;
		if(req.uri().startsWith("/")){
			uri = req.uri().substring(1);	
		} else {
			uri = req.uri();
		}
		
		System.out.println("URI " + uri);		
		//common
		if (uri.equalsIgnoreCase(PING)) {
			req.response().end("PONG");
			return true;
		}
		else if (uri.startsWith(redirectClickPrefix)) {
			RedirectUtil.redirect(uri, req);
			return true;
		}		
		//just for dev
		else if(uri.startsWith(logItemTracking)){
			//handle request for ITEM TRACKING				
			ResponseUtil.logRequestToKafka(req, logItemTracking);
			return true;
		}	
		else if(uri.startsWith(logUserActivity)){
			//handle request for ITEM TRACKING				
			ResponseUtil.logRequestToKafka(req, logUserActivity);
			return true;
		}	
		return false;
	}

}
