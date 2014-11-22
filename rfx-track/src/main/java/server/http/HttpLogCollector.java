package server.http;

import org.vertx.java.core.Handler;
import org.vertx.java.core.http.HttpServerRequest;

import rfx.core.stream.node.worker.BaseWorker;
import server.http.util.RedirectUtil;
import server.http.util.ResponseUtil;
import server.kafka.HttpLogKafkaHandler;

public class HttpLogCollector extends BaseWorker {
	
	public static final String version = "Reactive HttpLogCollector - version 1.0";
	
	public static final String logItemTrackingKafka = "kafka-item-tracking-Producer";

	static final String redirectClickPrefix = "/r/";

	public HttpLogCollector(String name) {
		super(name);
	}
	
	@Override
	public void start(String host, int port) {

		registerWorkerHttpHandler(host, port, new Handler<HttpServerRequest>() {
			@Override
			public void handle(HttpServerRequest req) {
				String uri = req.uri();
				System.out.println("URI " + uri);
				
				//common
				if (uri.startsWith(redirectClickPrefix)) {
					RedirectUtil.redirect(uri, req);
				} 				
				
				//just for dev
				else if(uri.startsWith("/item")){
					//handle request for ITEM TRACKING				
					ResponseUtil.logRequestToKafka(req, logItemTrackingKafka);
				}				
				else {
					req.response().end(version);
				}
			}
		});
	}

	@Override
	protected void onStartDone() {
		System.out.println("Ready to do my work!");
	}

	public static void main(String[] args) {
		String host = "127.0.0.1";
		int port = 14002;
		String name = host + "_" + port;
		HttpLogKafkaHandler.initKafkaSession();
		BaseWorker worker = new HttpLogCollector(name);
		worker.start(host, port);
	}
}
