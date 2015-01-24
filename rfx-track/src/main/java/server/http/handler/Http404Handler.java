package server.http.handler;

import org.apache.http.HttpStatus;
import org.vertx.java.core.http.HttpServerRequest;

public class Http404Handler implements BaseHttpHandler{
	private static final String s404 = "Not found for uri: %s";
	
	@Override
	public void handle(HttpServerRequest req) {
		req.response().setStatusCode(HttpStatus.SC_NOT_FOUND).end(String.format(s404, req.uri()));		
	}

	@Override
	public String getPathKey() {
		return null;
	}	
}
