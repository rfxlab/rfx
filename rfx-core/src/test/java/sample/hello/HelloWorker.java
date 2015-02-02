package sample.hello;

import java.util.TimerTask;

import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.net.NetSocket;

import redis.clients.jedis.ShardedJedisPool;
import redis.clients.jedis.exceptions.JedisException;
import rfx.core.model.WorkerTimeLog;
import rfx.core.nosql.jedis.RedisCommand;
import rfx.core.stream.cluster.ClusterDataManager;
import rfx.core.stream.node.worker.BaseWorker;
import rfx.core.util.StringPool;
import rfx.core.util.StringUtil;

import com.google.gson.Gson;

public class HelloWorker extends BaseWorker {

    public HelloWorker(String name) {
        super(name);
    }

    @Override
    public void start(String host, int port) {
    	 registerWorkerTcpHandler(host, port+1, new Handler<NetSocket>() {
 			@Override
 			public void handle(final NetSocket event) {
 				event.dataHandler(new Handler<Buffer>() {
 					public void handle(Buffer buffer) {
 						System.out.println(buffer.toString());
 						event.write("ok");
 					}
 				});
 			}
 		});
    	
        Handler<HttpServerRequest> handler = new Handler<HttpServerRequest>() {

            public void handle(HttpServerRequest request) {
                if (request.absoluteURI().getPath().equals("/cmd/kill")) {
                    request.response().end("Exiting...");
                    killWorker();
                    return;
                } else if (request.absoluteURI().getPath().equals("/cmd/ping")) {
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
        String host = "localhost";//args[0];
        int port = 8080;//StringUtil.safeParseInt(args[1]);
        String name = host + "_" + port;

        BaseWorker worker = new HelloWorker(name);
        worker.start(host, port);
    }
}
