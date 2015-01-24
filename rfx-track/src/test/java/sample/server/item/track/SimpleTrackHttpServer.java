package sample.server.item.track;

import java.util.HashSet;
import java.util.Set;

import rfx.core.util.StringUtil;
import server.http.RfxEventTrackingWorker;
import server.http.handler.BaseHttpHandler;

public class SimpleTrackHttpServer {
	public static void main(String[] args) throws Exception {
		if(args.length == 0 ){
			args = new String[]{"127.0.0.1","8080"};
		}		
		String host = args[0];
		int port = StringUtil.safeParseInt(args[1]);
		String name = host + "_" + port;		
		
		Set<BaseHttpHandler> routes = new HashSet<>();
		routes.add(new SimpleHttpLogHandler());
		RfxEventTrackingWorker.createHttpLogCollector(name, host, port, routes);		 
	}
}
