package server.http.handler;

import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.net.NetSocket;

import server.http.model.HttpEventKafkaLog;
import server.http.util.LogHandlerUtil;

import com.google.gson.Gson;

public class DefaultTrackingTcpHandler implements Handler<NetSocket> {

	private static final String OK = "ok";

	@Override
	public void handle(final NetSocket event) {		
		event.dataHandler(new Handler<Buffer>() {
			public void handle(Buffer buffer) {
				String json = buffer.toString();
				HttpEventKafkaLog el = new Gson().fromJson(json, HttpEventKafkaLog.class);
				LogHandlerUtil.logRequestToKafka(el);
				event.write(OK);
			}
		});
	}

}
