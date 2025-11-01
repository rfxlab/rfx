package sample.hello;

import java.util.Date;
import java.util.TimerTask;

import io.vertx.core.Handler;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.JedisException;
import rfx.core.model.WorkerData;
import rfx.core.nosql.jedis.RedisCommand;
import rfx.core.stream.cluster.ClusterDataManager;
import rfx.core.stream.node.worker.BaseWorker;

public class TestHttpWorker extends BaseWorker {

	public TestHttpWorker(String name) {
		super(name);
	}

	public void start(String host, int port) {
		Handler<HttpServerRequest> handler = new Handler<HttpServerRequest>() {

			public void handle(HttpServerRequest request) {

				JedisPool jedisPool = ClusterDataManager.getJedisClient();

				System.out.println("\nBEGIN RedisCommand");
				new RedisCommand<Boolean>(jedisPool) {

					@Override
					protected Boolean build(Jedis jedis) throws JedisException {
						System.out.println("jedisPool \n " + jedis.info().split("\n")[1]);

						Pipeline p = jedis.pipelined();

						String usersession = "test1";
						p.hset(usersession, "key1", "value1");
						p.hset(usersession, "key2", "value2");
						p.expire(usersession, 5);
						p.sync();

						System.out.println("DONE RedisCommand");
						return true;
					}
				}.executeAsync();
				System.out.println("END RedisCommand \n ");

				if (request.path().equals("/cmd/kill")) {
					request.response().end("Exiting...");
					killWorker();
					return;
				} else if (request.path().equals("/cmd/ping")) {
					request.response().end("PONG");
					return;
				}
				else if (request.path().equals("/cmd/redis-info")) {
					HttpServerResponse response = request.response();
					new RedisCommand<Boolean>(jedisPool) {

						@Override
						protected Boolean build(Jedis jedis) throws JedisException {
							String info = jedis.info().split("\n")[1];
							System.out.println("jedis.info  " + info);
							
							response.end(info);
							return true;
						}
					}.executeAsync((rs)->{
						
					}, null);
					
					return;
				}
				request.response().end("I love you at " + new Date());
			}
		};
		registerWorkerHttpHandler(host, port, handler);
	}

	@Override
	protected void onStartDone() {
		System.out.println("Ready to do my work!");

		timer.schedule(new TimerTask() {

			@Override
			public void run() {
				ClusterDataManager.updateWorkerData(publicHost, publicPort, WorkerData.Status.STARTED);

			}
		}, 2000, 2000);
	}

	public static void main(String[] args) {
		String host = "localhost";// args[0];
		int port = 8082;// StringUtil.safeParseInt(args[1]);
		String name = host + "_" + port;

		BaseWorker worker = new TestHttpWorker(name);
		worker.start(host, port);
	}
}
