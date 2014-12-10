package sample.server.item.track;

import org.vertx.java.core.http.HttpServerRequest;

import server.http.handler.BaseHttpHandler;
import server.http.util.RedirectUtil;
import server.http.util.ResponseUtil;

public class ItemTrackHttpHandler extends BaseHttpHandler {
	
	private static final String ITEM = "/item";
	private static final String PING = "/ping";
	static final String logItemTrackingKafka = "tk";
	static final String redirectClickPrefix = "/r/";

	@Override
	public boolean handle(HttpServerRequest req) {
		String uri = req.uri();
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
		else if(uri.startsWith(ITEM)){
			//handle request for ITEM TRACKING				
			ResponseUtil.logRequestToKafka(req, logItemTrackingKafka);
			return true;
		}	
		return false;
	}

}
