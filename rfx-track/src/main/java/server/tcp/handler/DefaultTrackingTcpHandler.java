package server.tcp.handler;

import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetSocket;
import server.http.util.KafkaLogHandlerUtil;

public class DefaultTrackingTcpHandler implements Handler<NetSocket> {

	private static final String OK = "ok";

	@Override
	public void handle(final NetSocket event) {		
		event.handler(new Handler<Buffer>() {
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
