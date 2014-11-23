package server.http;

import org.vertx.java.core.Handler;
import org.vertx.java.core.http.HttpServerRequest;

import rfx.core.stream.node.worker.BaseWorker;
import rfx.core.util.StringUtil;
import rfx.core.util.Utils;
import server.http.configs.KafkaProducerConfigs;
import server.http.handler.BaseHttpHandler;
import server.kafka.HttpLogKafkaHandler;

public class HttpLogCollector extends BaseWorker {	
	public static final String version = "Reactive HttpLogCollector - version 1.0";
	static KafkaProducerConfigs kafkaProducerConfigs = KafkaProducerConfigs.load();
	
	public static void createHttpLogCollector(String name, String host, int port){
		HttpLogKafkaHandler.initKafkaSession();
		BaseWorker worker = new HttpLogCollector(name);
		worker.start(host, port);		
	}
	
	protected HttpLogCollector(String name) {		
		super(name);
	}
	
	@Override
	public void start(String host, int port) {
		BaseHttpHandler theHandler;
		try {
			String className = kafkaProducerConfigs.getDefaultHttpHandlerClass();
			theHandler = (BaseHttpHandler)Class.forName(className).newInstance();
		} catch (Exception e) {			
			e.printStackTrace();
			System.out.println("defaultHttpHandlerClass is NULL");
			Utils.exitSystemAfterTimeout(500);
			return;
		} 
		if(theHandler == null){
			return;
		}
		registerWorkerHttpHandler(host, port, new Handler<HttpServerRequest>() {
			@Override
			public void handle(HttpServerRequest req) {				
				boolean processed = theHandler.handle(req);
				if( ! processed ){
					req.response().end(version);
				}
			}
		});
	}

	@Override
	protected void onStartDone() {
		System.out.println(getClass().getName() + " started OK!");
	}
	
	public static void main(String[] args) {
		if(args.length < 2){
			System.out.println("args.length < 2, need host port to run");
			return;
		}
		String host = args[0];
		int port = StringUtil.safeParseInt(args[1]);
		String name = host + "_" + port;		
		HttpLogCollector.createHttpLogCollector(name, host, port);		 
	}
	
}
