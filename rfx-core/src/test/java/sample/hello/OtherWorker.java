package sample.hello;

import java.util.Date;
import java.util.TimerTask;

import com.google.gson.Gson;

import io.vertx.core.Handler;
import io.vertx.core.http.HttpServerRequest;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.JedisException;
import rfx.core.model.WorkerTimeLog;
import rfx.core.nosql.jedis.RedisCommand;
import rfx.core.stream.cluster.ClusterDataManager;
import rfx.core.stream.node.worker.BaseWorker;
import rfx.core.util.StringUtil;

public class OtherWorker extends BaseWorker {

	public OtherWorker(String name) {
		super(name);
	}

	public void start(String host, int port) {
		Handler<HttpServerRequest> handler = new Handler<HttpServerRequest>() {

			public void handle(HttpServerRequest request) {
				
				JedisPooled jedisPool = ClusterDataManager.getJedisClient();
				new RedisCommand<Boolean>(jedisPool) {

					@Override
					protected Boolean build() throws JedisException {
						System.out.println("BEGIN RedisCommand");

						String workerName = StringUtil.toString(publicHost.replaceAll("\\.", ""), "_", publicPort);
						WorkerTimeLog timeLog = new Gson().fromJson(jedis.hget(ClusterDataManager.CLUSTER_WORKER_PREFIX,
								workerName + ClusterDataManager.WORKER_TIMELOG_POSTFIX), WorkerTimeLog.class);
						if (timeLog == null) {
							timeLog = new WorkerTimeLog();
						}
						timeLog.addUpTime(System.currentTimeMillis());
						Pipeline p = jedis.pipelined();
						
						p.hset(ClusterDataManager.CLUSTER_WORKER_PREFIX,
								workerName + ClusterDataManager.WORKER_TIMELOG_POSTFIX, new Gson().toJson(timeLog));
						
						String usersession = "test1";
						p.hset(usersession, "key1", "value1");
						p.hset(usersession, "key2", "value2");
						p.expire(usersession, 5);
						p.sync();
						
	
						
						System.out.println("END RedisCommand");
						return true;
					}
				}.executeAsync();

				if (request.path().equals("/cmd/kill")) {
					request.response().end("Exiting...");
					killWorker();
					return;
				} else if (request.path().equals("/cmd/ping")) {
					request.response().end("PONG");
					return;
				}
				request.response().end("Hello at " + new Date());
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
				//ClusterDataManager.updateWorkerData(publicHost, publicPort);
				
				
			}
		}, 2000, 2000);
	}

	public static void main(String[] args) {
		String host = "localhost";// args[0];
		int port = 8082;// StringUtil.safeParseInt(args[1]);
		String name = host + "_" + port;

		BaseWorker worker = new OtherWorker(name);
		worker.start(host, port);
	}
}
