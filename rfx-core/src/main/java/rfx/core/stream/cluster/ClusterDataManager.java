package rfx.core.stream.cluster;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;
import rfx.core.model.WorkerData;
import rfx.core.model.WorkerInfo;
import rfx.core.nosql.jedis.RedisClientFactory;
import rfx.core.nosql.jedis.RedisCommand;
import rfx.core.util.StringUtil;

/**
 * @author Trieu Nguyen
 * @since 2025
 *
 */
public final class ClusterDataManager {

	private static final Logger logger = LoggerFactory.getLogger(ClusterDataManager.class);

	private static final String CLUSTER_INFO_REDIS = "clusterInfoRedis";
	public static final String CLUSTER_WORKER_PREFIX = "workers";
	public static final String WORKER_INFO_POSTFIX = ".info";
	public static final String WORKER_DATA_POSTFIX = ".data";
	public static final String WORKER_TIMELOG_POSTFIX = ".timelog";

	private static Vertx vertx = null;

	/** one pooled, thread-safe client shared across app */
	private static final JedisPool JEDIS_POOL = RedisClientFactory.buildRedisPool(CLUSTER_INFO_REDIS);
	protected static int cpuCores = Runtime.getRuntime().availableProcessors();
	public static final long MAX_TIMEOUT_WORKER = 90000000000L;

	public static JedisPool getJedisClient() {
		return JEDIS_POOL;
	}

	public synchronized final static Vertx theVertx() {
		if (vertx == null) {
			// disable the creation of file-cache folders ".vertx"
			System.setProperty("vertx.disableFileCPResolving", "true");

			// refer http://vertx.io/manual.html#performance-tuning
			// DeploymentOptions options = new DeploymentOptions().setWorker(true);
			VertxOptions options = new VertxOptions();
			options.setMaxEventLoopExecuteTime(MAX_TIMEOUT_WORKER);
			options.setBlockedThreadCheckInterval(5000) // In milliseconds
					.setBlockedThreadCheckIntervalUnit(TimeUnit.MILLISECONDS);
			options.setWorkerPoolSize(cpuCores * 10);
			options.setEventLoopPoolSize(cpuCores * 2);
			options.setPreferNativeTransport(true);
			options.setMaxWorkerExecuteTime(100L).setMaxWorkerExecuteTimeUnit(TimeUnit.SECONDS);
			options.setHAEnabled(true).setQuorumSize(2); // The number of nodes that must remain for the system to
															// operate

			vertx = Vertx.vertx(options);
		}

		return vertx;
	}

	// ------------------------------------------------------------------------

	public static boolean saveWorkerInfo(WorkerInfo workerInfo) {
		new RedisCommand<Void>(JEDIS_POOL) {
			@Override
			protected Void build(Jedis jedis) throws JedisException {
				jedis.hset(CLUSTER_WORKER_PREFIX, workerInfo.getName() + WORKER_INFO_POSTFIX, workerInfo.toJson());
				return null;
			}
		}.executeAsync();
		return true;
	}

	public static Map<String, WorkerInfo> getWorkerInfoFromRedis() {
		return new RedisCommand<Map<String, WorkerInfo>>(JEDIS_POOL) {
			@Override
			protected Map<String, WorkerInfo> build(Jedis jedis) throws JedisException {
				Map<String, WorkerInfo> mapWorkerInfo = new HashMap<>();
				Map<String, String> map = jedis.hgetAll(CLUSTER_WORKER_PREFIX);
				for (Map.Entry<String, String> entry : map.entrySet()) {
					String key = entry.getKey();
					if (key.endsWith(WORKER_INFO_POSTFIX)) {
						String json = entry.getValue();
						if (StringUtil.isNotEmpty(json)) {
							String name = key.replace(WORKER_INFO_POSTFIX, "");
							mapWorkerInfo.put(name, WorkerInfo.fromJson(json));
						}
					}
				}
				return mapWorkerInfo;
			}
		}.execute();
	}

	public static boolean ping(WorkerInfo workerInfo) {
		try (Socket socket = new Socket()) {
			socket.connect(new InetSocketAddress(workerInfo.getHost(), workerInfo.getPort()), 300);
			return true;
		} catch (Exception ex) {
			return false;
		}
	}

	/** worker node updates its own memory usage etc. */
	public static void updateWorkerData(String host, int port, WorkerData.Status status) {
		logger.info("updateWorkerData " + host + ":" + port);
		new RedisCommand<Void>(JEDIS_POOL) {
			@Override
			protected Void build(Jedis jedis) throws JedisException {
				String workerName = StringUtil.toString(host.replaceAll("\\.", ""), "_", port);
				Gson gson = new Gson();

				WorkerData workerData = gson.fromJson(
						jedis.hget(CLUSTER_WORKER_PREFIX, workerName + WORKER_DATA_POSTFIX), WorkerData.class);

				if (workerData == null) {
					workerData = new WorkerData();
				}

				Runtime rt = Runtime.getRuntime();
				workerData.setMemoryUsage(readableFileSize(rt.totalMemory() - rt.freeMemory()));
				workerData.setMemoryLimit(readableFileSize(rt.maxMemory()));
				workerData.setStatus(status);

				jedis.hset(CLUSTER_WORKER_PREFIX, workerName + WORKER_DATA_POSTFIX, gson.toJson(workerData));
				return null;
			}
		}.executeAsync();
	}

	public static String readableFileSize(long size) {
		if (size <= 0)
			return "0";
		final String[] units = { "B", "KB", "MB", "GB", "TB" };
		int digitGroups = (int) (Math.log10(size) / Math.log10(1024));
		return new DecimalFormat("#,##0.#").format(size / Math.pow(1024, digitGroups)) + " " + units[digitGroups];
	}
}
