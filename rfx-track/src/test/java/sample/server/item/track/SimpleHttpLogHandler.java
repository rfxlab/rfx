package sample.server.item.track;

import org.vertx.java.core.http.HttpServerRequest;

import rfx.core.util.StringPool;
import server.http.handler.BaseHttpHandler;
import server.http.util.KafkaLogHandlerUtil;
import server.http.util.RedirectUtil;

public class SimpleHttpLogHandler implements BaseHttpHandler {
		
	private static final String PONG = "PONG";
	private static final String DATA = "data";
	private static final String FAVICON_ICO = "favicon.ico";
	private static final String LOG_DATA = "log-data";
	private static final String PING = "ping";
	static final String logItemTracking = "tk";
	static final String logUserActivity = "u";
	static final String logUserClick = "c";
	static final String redirectClickPrefix = "r/";

	@Override
	public void handle(HttpServerRequest req) {
		String uri;
		if(req.uri().startsWith("/")){
			uri = req.uri().substring(1);	
		} else {
			uri = req.uri();
		}
		
		System.out.println("URI: " + uri);		
		//common
		if(uri.startsWith(LOG_DATA)){			
			String json = req.params().get(DATA);
			KafkaLogHandlerUtil.logDataToKafka(req,json);
		} 
		else if (uri.equalsIgnoreCase(FAVICON_ICO)) {
			KafkaLogHandlerUtil.trackingResponse(req);
		}
		else if (uri.equalsIgnoreCase(PING)) {
			req.response().end(PONG);
		}
		else if (uri.startsWith(redirectClickPrefix)) {
			RedirectUtil.redirect(uri, req);
		}		
		//just for dev
		else if(uri.startsWith(logItemTracking)){
			//handle request for ITEM TRACKING				
			KafkaLogHandlerUtil.logHttpRequestToKafka(req, logItemTracking);
		}	
		else if(uri.startsWith(logUserActivity)){
			//handle request for ITEM TRACKING				
			KafkaLogHandlerUtil.logHttpRequestToKafka(req, logUserActivity);			
		} 
		else if(uri.startsWith(logUserClick)){
			//handle request for ITEM TRACKING				
			KafkaLogHandlerUtil.logHttpRequestToKafka(req, logUserClick);			
		} 
	}

	@Override
	public String getPathKey() {
		return StringPool.STAR;
	}

}
