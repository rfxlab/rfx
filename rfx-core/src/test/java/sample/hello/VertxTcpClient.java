package sample.hello;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.VertxFactory;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.net.NetClient;
import org.vertx.java.core.net.NetSocket;

import rfx.core.util.Utils;

public class VertxTcpClient {

	public static void main(String[] args) throws Exception {
		NetClient client = VertxFactory.newVertx().createNetClient();
		client.connect(14003, "localhost",
				new Handler<AsyncResult<NetSocket>>() {
					@Override
					public void handle(AsyncResult<NetSocket> event) {
						if (event.succeeded()) {
							System.out.println("connect ok");
							Utils.sleep(5000);
							System.out.println("sending ...");
							event.result()
									.write("This is a message from client")
									.dataHandler(new Handler<Buffer>() {
										@Override
										public void handle(Buffer event) {
											System.out
													.println("Got from server "
															+ event.toString());
										}
									})
									.closeHandler(new Handler<Void>() {										
										@Override
										public void handle(Void event) {
											System.out.println("connection is closed!");
										}
									});
						} else {
							System.out.println("connect fail");
						}
					}
				});

		Utils.sleep(10000);
	}
}
