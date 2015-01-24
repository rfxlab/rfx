package server.http;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.http.HttpStatus;
import org.vertx.java.core.Handler;
import org.vertx.java.core.http.HttpServerRequest;

import rfx.core.configs.WorkerConfigs;
import rfx.core.stream.node.worker.BaseWorker;
import rfx.core.util.StringPool;
import server.http.handler.BaseHttpHandler;
import server.http.handler.Http404Handler;
import server.http.handler.PingHttpHandler;
import server.http.handler.kafka.KafkaHttpEventLogHandler;

/**
 * The HTTP server instance 
 * 
 * @author trieunt
 *
 */
public class RfxEventTrackingWorker extends BaseWorker {	
	
	private static final Map<String, BaseHttpHandler> cachedRoutes = new ConcurrentHashMap<>();
	private static final Http404Handler http404Handler = new Http404Handler();	
	
	public static void createHttpLogCollector(String name, String host, int port,Set<BaseHttpHandler> routes) {
		BaseWorker worker = new RfxEventTrackingWorker(name, routes);
		worker.start(host, port);		
	}
	
	public static void createHttpLogCollector(String name, String host, int port)  {
		Set<BaseHttpHandler> routes = new HashSet<>(2);
		routes.add(new PingHttpHandler());
		routes.add(new KafkaHttpEventLogHandler());		
		BaseWorker worker = new RfxEventTrackingWorker(name, routes);		
		worker.start(host, port);
	}
	
	protected RfxEventTrackingWorker(String name, Set<BaseHttpHandler> routes) {		
		super(name);
		setupRoutes(routes);
	}	
		
	private static void setupRoutes(Set<BaseHttpHandler> routes) {
		for (BaseHttpHandler h : routes) {
			if(h.getPathKey().equals(StringPool.STAR)){
				cachedRoutes.clear();
				cachedRoutes.put(h.getPathKey(), h);
				System.out.println(String.format("\t uriPath: %s => %s", h.getPathKey(), h.getClass().getName()));
				break;
			} 
			cachedRoutes.put(h.getPathKey(), h);
			System.out.println(String.format("\t uriPath: %s => %s", h.getPathKey(), h.getClass().getName()));
		}
	}
	
	private static BaseHttpHandler findHandler(String path) throws Exception {
		final BaseHttpHandler matchAllHandler = cachedRoutes.get(StringPool.STAR);		
		if(matchAllHandler != null){
			return matchAllHandler;
		}
		final BaseHttpHandler h = cachedRoutes.get(path);
		if(h != null){
			return h;
		}
		return http404Handler;
	}
	
	@Override
	public void start(String host, int port) {
		registerWorkerHttpHandler(host, port, new Handler<HttpServerRequest>() {
			@Override
			public void handle(HttpServerRequest req) {	
				String path = req.path();				
				try {
					findHandler(path).handle(req);
				} catch (Throwable e) {
					String err = e.getMessage();
					req.response().setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR).end(err);
				}
			}
		});		
	}

	@Override
	protected void onStartDone() {
		System.out.println(getClass().getName() + " started OK!");
	}	
		
	public static void main(String[] args) throws Exception {
		//LogUtil.setDebug(true);
		WorkerConfigs configs = WorkerConfigs.load();
		String host = configs.getHostName();
		int port = configs.getAllocatedWorkers().get(0).getPort();
		String name = host + "_" + port;		
		RfxEventTrackingWorker.createHttpLogCollector(name, host, port);		 
	}	
}