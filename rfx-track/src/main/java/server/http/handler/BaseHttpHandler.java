package server.http.handler;

import org.vertx.java.core.http.HttpServerRequest;

/**
 * the interface for handling HTTP request
 * 
 * @author trieu
 *
 */
public interface BaseHttpHandler {	
	public void handle(HttpServerRequest req);	
	public String getPathKey();
}