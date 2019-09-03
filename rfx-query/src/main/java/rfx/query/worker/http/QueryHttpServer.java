package rfx.query.worker.http;

import org.vertx.java.core.Handler;
import org.vertx.java.core.http.HttpServerRequest;

import rfx.core.stream.node.worker.BaseWorker;

public class QueryHttpServer  extends BaseWorker {

	private static final String HELLO = "Hello";

	public QueryHttpServer(String name) {
		super(name);
	}


    @Override
    public void start(String host, int port) {
        Handler<HttpServerRequest> handler = new Handler<HttpServerRequest>() {
            public void handle(HttpServerRequest request) {                
                request.response().end(HELLO);
            }
        };
       
        super.checkAndCreateHttpServer(host, port);
    }
    
    @Override
    protected void onStartDone() {
        System.out.println("Ready to do my work!");
    }
    
    public static void main(String[] args) {
        String host = "127.0.0.1";
        int port = 3001;
        String name = host + "_" + port;

        BaseWorker worker = new QueryHttpServer(name);
        worker.start(host, port);
    }
}
