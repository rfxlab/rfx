package sample.server.item.track;

import server.http.HttpLogCollector;

public class ItemTrackHttpServer {
	public static void main(String[] args) {
		String host = "127.0.0.1";
		int port = 14002;
		String name = host + "_" + port;		
		HttpLogCollector.createHttpLogCollector(name, host, port);		 
	}
}
