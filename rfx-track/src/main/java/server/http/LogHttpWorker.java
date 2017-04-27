package server.http;

import org.apache.http.HttpStatus;

import io.vertx.core.Handler;
import io.vertx.core.http.HttpServerRequest;
import rfx.core.stream.node.worker.BaseWorker;
import server.http.handler.BaseHttpHandler;

/**
 * Worker class for creating new Log HTTP Server with implemented BaseHttpHandler
 * 
 * @author trieunt
 *
 */
public class LogHttpWorker extends BaseWorker {
	
	final BaseHttpHandler httpHandler;

	protected LogHttpWorker(String name, BaseHttpHandler httpHandler) {
		super(name);
		this.httpHandler = httpHandler; 
	}
	
	static String getName(String host, int port){
		return LogHttpWorker.class.getSimpleName() + "_" + host + "_" + port;
	}
	
	/**
	 * Create new Customized HTTP Log Server instance with implemented httpHandler
	 * 
	 * @param host
	 * @param port
	 * @param httpHandler
	 */
	public static void startNewInstance(String host, int port,BaseHttpHandler httpHandler) {
		new LogHttpWorker(getName(host, port), httpHandler ).start(host, port);
	}
	
	@Override
	public void start(String host, int port) {
		registerWorkerHttpHandler(host, port, new Handler<HttpServerRequest>() {
			@Override
			public void handle(HttpServerRequest req) {
				try {
					httpHandler.handle(req);
				} catch (Throwable e) {
					String err = e.getMessage();
					req.response().setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR).end(err);
				}
			}
		});		
	}

}
