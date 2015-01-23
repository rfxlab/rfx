package sample.server.item.track;

import rfx.core.util.StringUtil;
import server.http.HttpLogCollector;

public class ItemTrackHttpServer {
	public static void main(String[] args) {
		if(args.length == 0 ){
			args = new String[]{"127.0.0.1","8080"};
		}		
		String host = args[0];
		int port = StringUtil.safeParseInt(args[1]);
		String name = host + "_" + port;		
		HttpLogCollector.createHttpLogCollector(name, host, port);		 
	}
}
