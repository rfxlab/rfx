package sample.hello;

import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.NetSocket;
import rfx.core.stream.node.worker.BaseWorker;

public class HelloWorker extends BaseWorker {

	public HelloWorker(String name) {
		super(name);
	}

	@Override
	public void start(String host, int port) {
		registerWorkerTcpHandler(host, port + 1, new Handler<NetSocket>() {
			@Override
			public void handle(final NetSocket event) {
				event.handler(new Handler<Buffer>() {
					public void handle(Buffer buffer) {
						System.out.println(buffer.toString());
						event.write("ok");
					}
				});
			}
		});

		Handler<HttpServerRequest> handler = new Handler<HttpServerRequest>() {
			public void handle(HttpServerRequest request) {
				String absoluteURI = request.path();
				System.out.println(absoluteURI);
				if (absoluteURI.equals("/cmd/kill")) {
					request.response().end("Exiting...");
					killWorker();
					return;
				} else if (absoluteURI.equals("/cmd/ping")) {
					request.response().end("PONG");
					return;
				}
				request.response().end("Hello");
			}
		};
		registerWorkerHttpHandler(host, port, handler);
	}

	@Override
	protected void onStartDone() {
		System.out.println("onStartDone Ready to do my work!");
	}

	public static void main(String[] args) {
		String host = "localhost";// args[0];
		int port = 8080;// StringUtil.safeParseInt(args[1]);
		String name = host + "_" + port;

		BaseWorker worker = new HelloWorker(name);
		worker.start(host, port);
	}
}
