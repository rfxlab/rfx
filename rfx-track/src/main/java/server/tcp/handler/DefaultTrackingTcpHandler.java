package server.tcp.handler;

import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.net.NetSocket;

import rfx.core.util.Utils;
import server.http.model.HttpEventKafkaLog;
import server.http.util.KafkaLogHandlerUtil;

import com.google.gson.Gson;

public class DefaultTrackingTcpHandler implements Handler<NetSocket> {

	private static final String OK = "ok";

	@Override
	public void handle(final NetSocket event) {		
		event.dataHandler(new Handler<Buffer>() {
			public void handle(Buffer buffer) {				
				
				String json = new String(buffer.getBytes());
				System.out.println(json);
				System.out.println();
				System.out.println(buffer.toString());
				
				//Utils.sleep(100);				
				KafkaLogHandlerUtil.logRequestToKafka(json);
				event.write(OK);
			}
		});
	}

}
