package sample.hello;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetSocket;
import rfx.core.util.Utils;

public class VertxTcpClient {

	public static void main(String[] args) throws Exception {
		NetClient client = Vertx.vertx().createNetClient();
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
									.handler(new Handler<Buffer>() {
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
