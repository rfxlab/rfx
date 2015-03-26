package server.http;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.http.HttpStatus;
import org.vertx.java.core.Handler;
import org.vertx.java.core.http.HttpServerRequest;

import rfx.core.stream.node.worker.BaseWorker;
import rfx.core.util.StringPool;
import rfx.core.util.StringUtil;
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
	
	static String getName(String host, int port){
		return RfxEventTrackingWorker.class.getSimpleName() + "_" + host + "_" + port;
	}
	
	/**
	 * Create new Customized HTTP Log Server instance with implemented routes
	 * 
	 * @param host
	 * @param port
	 * @param routes
	 */
	public static void createHttpLogCollector(String host, int port,Set<BaseHttpHandler> routes) {		
		BaseWorker worker = new RfxEventTrackingWorker(getName(host, port), routes);
		worker.start(host, port);		
	}
	
	/**
	 * Create new Customized HTTP Log Server instance with implemented httpHandler
	 * 
	 * @param host
	 * @param port
	 * @param httpHandler
	 */
	public static void createHttpLogCollector(String host, int port,BaseHttpHandler httpHandler) {		
		Set<BaseHttpHandler> routes = new HashSet<>(1);
		routes.add(httpHandler);
		BaseWorker worker = new RfxEventTrackingWorker(getName(host, port), routes );
		worker.start(host, port);		
	}
	
	/**
	 * Create default HTTP Log Server instance
	 * 
	 * @param host
	 * @param port
	 */
	public static void createHttpLogCollector(String host, int port)  {		
		Set<BaseHttpHandler> routes = new HashSet<>(2);
		routes.add(new PingHttpHandler());
		routes.add(new KafkaHttpEventLogHandler());		
		BaseWorker worker = new RfxEventTrackingWorker(getName(host, port), routes);		
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
		if(args.length == 0 ){
			args = new String[]{"127.0.0.1","8080"};
		}		
		String host = args[0];
		int port = StringUtil.safeParseInt(args[1]);		
		RfxEventTrackingWorker.createHttpLogCollector(host, port);		 
	}	
}