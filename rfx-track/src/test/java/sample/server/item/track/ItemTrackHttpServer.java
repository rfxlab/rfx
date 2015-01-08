package sample.server.item.track;

import rfx.core.util.StringUtil;
import server.http.HttpLogCollector;

public class ItemTrackHttpServer {
	public static void main(String[] args) {
		String host = args[0];
		int port = StringUtil.safeParseInt(args[1]);
		String name = host + "_" + port;		
		HttpLogCollector.createHttpLogCollector(name, host, port);		 
	}
}
