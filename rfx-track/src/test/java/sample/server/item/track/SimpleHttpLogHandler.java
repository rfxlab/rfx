package sample.server.item.track;

import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpServerRequest;
import rfx.core.util.DateTimeUtil;
import rfx.core.util.StringPool;
import rfx.core.util.StringUtil;
import server.http.handler.BaseHttpHandler;
import server.http.util.HttpTrackingUtil;
import server.http.util.KafkaLogHandlerUtil;
import server.http.util.RedirectUtil;

public class SimpleHttpLogHandler implements BaseHttpHandler {

    private static final String REDIRECT_PREFIX = "/r/";

    private static final String LOG_CLICK = "l";
    private static final String PONG = "PONG";
    private static final String DATA = "data";
    private static final String FAVICON_ICO = "favicon.ico";
    private static final String LOG_DATA = "log-data";
    private static final String PING = "ping";
    static final String logItemTracking = "tk";
    static final String logUserActivity = "u";
    static final String logUserClick = "c";
    static final String redirectClickPrefix = "r";
    static final String monitorDataPrefix = "md";

    @Override
    public void handle(HttpServerRequest req) {
	String uri;
	if (req.uri().startsWith("/")) {
	    uri = req.uri().substring(1);
	} else {
	    uri = req.uri();
	}

	System.out.println("URI: " + uri);

	// common
	if (uri.startsWith(LOG_DATA)) {
	    String json = req.params().get(DATA);
	    KafkaLogHandlerUtil.logDataToKafka(req, json);
	} else if (uri.equalsIgnoreCase(FAVICON_ICO)) {
	    HttpTrackingUtil.trackingResponse(req);
	} else if (uri.equalsIgnoreCase(PING)) {
	    req.response().end(PONG);
	} else if (uri.startsWith(redirectClickPrefix)) {
	    MultiMap params = req.params();
	    String link = params.get("link");
	    int index = -1;
	    if (StringUtil.isNotEmpty(link)) {
		index = link.indexOf(REDIRECT_PREFIX);
	    }
	    if (index > 0) {
		System.out.println("link " + link);
		String clickUrl = link.substring(index + 3);
		System.out.println("clickUrl " + clickUrl);
		KafkaLogHandlerUtil.log(req, logUserClick);
		RedirectUtil.redirect(clickUrl, req);
		// KafkaLogHandlerUtil.trackingResponse(req);
	    } else {
		RedirectUtil.redirect(uri, req);
	    }
	    RealtimeTrackingUtil.updateKafkaLogEvent(DateTimeUtil.currentUnixTimestamp(), "click-redirect");
	}
	// just for dev
	else if (uri.startsWith(logItemTracking)) {
	    // handle log request
	    KafkaLogHandlerUtil.logAndResponseImage1px(req, logItemTracking);
	} else if (uri.startsWith(logUserActivity)) {
	    // handle log request
	    KafkaLogHandlerUtil.logAndResponseImage1px(req, logUserActivity);
	} else if (uri.startsWith(LOG_CLICK)) {
	    // handle log request
	    KafkaLogHandlerUtil.logAndResponseImage1px(req, logUserClick);
	    RealtimeTrackingUtil.updateKafkaLogEvent(DateTimeUtil.currentUnixTimestamp(), "click-http");
	} else if (uri.startsWith(monitorDataPrefix)) {
	    String data;
	    if (req.params() != null) {
		String key = req.params().get("key");
		data = RealtimeTrackingUtil.getAllKafkaLogEvents(key);
	    } else {
		data = RealtimeTrackingUtil.getAllKafkaLogEvents(null);
	    }
	    req.response().end(data);
	} else {
	    req.response().end("Not handler found for uri:" + uri);
	}
    }

    @Override
    public String getPathKey() {
	return StringPool.STAR;
    }

}
