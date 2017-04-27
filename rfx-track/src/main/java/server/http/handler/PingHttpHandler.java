package server.http.handler;

import io.vertx.core.http.HttpServerRequest;

public class PingHttpHandler implements BaseHttpHandler{
	private static final String PONG = "PONG";
	public static final String PING = "/ping";
	
	@Override
	public void handle(HttpServerRequest req) {
		req.response().end(PONG);		
	}

	@Override
	public String getPathKey() {		
		return PING;
	}	
}
