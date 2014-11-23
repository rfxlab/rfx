package server.http.handler;

import org.vertx.java.core.http.HttpServerRequest;

public abstract class BaseHttpHandler {
	public abstract boolean handle(HttpServerRequest req);
}
