package rfx.core.stream.node.worker;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetSocket;
import rfx.core.model.WorkerData;
import rfx.core.stream.cluster.ClusterDataManager;
import rfx.core.util.Utils;

/**
 * the base class for worker implementation <br>
 * a worker could be a scheduled job, a kafka stream processor or a http web
 * service
 * 
 * @author trieunt <br>
 *
 */
public abstract class BaseWorker {

	private static final int SEND_BUFFER_SIZE = 256 * 1024; // 256 KB

	static Logger logger = LoggerFactory.getLogger(BaseWorker.class);

	private static final String URI_GET_SERVER_TIME = "/get/server-time";
	private static final String URI_GET_HOST = "/get/host";
	private static final String URI_GET_NAME = "/get/name";
	private static final String URI_GET_STATUS = "/get/status";
	private static final String URI_KILL = "/kill";
	private static final String URI_RESTART = "/restart";
	private static final String URI_PAUSE = "/pause";
	private static final String URI_PING = "/ping";

	protected String publicHost;
	protected int publicPort;
	protected String privateHost;
	protected int privatePort;
	protected String name;
	protected String classnameWorker = getClass().getName();
	protected WorkerData.Status status = WorkerData.Status.KILLED;
	protected boolean autoStart = true;
	protected Vertx vertxInstance;
	protected HttpServer httpServerInstance;

	protected Timer timer = new Timer(true);
	private static BaseWorker _worker;
	private final Map<String, Consumer<HttpServerResponse>> BASE_HANDLERS = createBaseHandlers();

	public static BaseWorker getInstance() {
		return _worker;
	}

	protected static void setWorker(BaseWorker worker) {
		_worker = worker;
	}

	public BaseWorker(String name) {
		super();
		this.name = name;
		this.classnameWorker = getClass().getName();
		status = WorkerData.Status.STARTING;
		vertxInstance = ClusterDataManager.theVertx();
		initBeforeStart();
	}

	public BaseWorker(String name, boolean autoStart) {
		this(name);
		this.autoStart = autoStart;
	}

	final public String getName() {
		return name;
	}

	final public WorkerData.Status getStatus() {
		return status;
	}

	final public String getPublicHost() {
		return publicHost;
	}

	final public int getPublicPort() {
		return publicPort;
	}

	final public String getPrivateHost() {
		return privateHost;
	}

	final public int getPrivatePort() {
		return privatePort;
	}

	@Override
	public String toString() {
		return name == null ? BaseWorker.class.getSimpleName() : name;
	}

	final protected boolean isAddressAlreadyInUse(String host, int port) {
		int timeout = 500;
		try {
			// logger.info(" check "+workerInfo.getName());
			Socket socket = new Socket();
			socket.connect(new InetSocketAddress(host, port), timeout);
			socket.close();
			return true;
		} catch (Exception ex) {
		}
		return false;
	}

	protected HttpServer checkAndCreateHttpServer(String host, int port) {

		if (isAddressAlreadyInUse(host, port)) {
			System.err.println(host + ":" + port + " isAddressAlreadyInUse!");
			Utils.exitSystemAfterTimeout(200);
			return null;
		}

		this.publicHost = host;
		this.publicPort = port;

		HttpServerOptions options = new HttpServerOptions()
				// Connection handling
				.setAcceptBacklog(20000)

				// Buffer sizes — critical for large payloads
				.setSendBufferSize(SEND_BUFFER_SIZE).setReceiveBufferSize(SEND_BUFFER_SIZE)

				// Netty pooled buffers (good for throughput)
				.setUsePooledBuffers(true)

				// Protect server from slowloris / stuck clients
				.setIdleTimeout(30) // seconds
				.setTcpKeepAlive(true)

				// HTTP behavior
				.setCompressionSupported(true).setDecompressionSupported(true);

		return vertxInstance.createHttpServer(options);
	}

	private NetServer checkAndCreateNetServer(String host, int port) {
		if (isAddressAlreadyInUse(host, port)) {
			System.err.println(host + ":" + port + " isAddressAlreadyInUse!");
			Utils.exitSystemAfterTimeout(200);
			return null;
		}
		try {
			this.privateHost = host;
			this.privatePort = port;
			return Vertx.vertx().createNetServer();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	final protected void registerWorkerHttpHandler(String host, int port, Handler<HttpServerRequest> handler) {
		HttpServer server = checkAndCreateHttpServer(host, port);
		if (server == null) {
			System.err.println("registerWorkerHttpHandler return NULL value");
			return;
		}
		server.requestHandler(handler).listen(port, host);
		registerWorkerNodeIntoCluster();
	}

	final protected void registerWorkerTcpHandler(String host, int port, Handler<NetSocket> handler) {
		NetServer server = checkAndCreateNetServer(host, port);
		if (server == null) {
			System.err.println("checkAndCreateNetServer return NULL value");
			return;
		}
		logger.info(String.format("...registerWorkerTcpHandler %s:%s", host, port));
		server.connectHandler(handler).listen(port, host);
		// TODO
		// registerWorkerNodeIntoCluster();
	}

	final synchronized protected void killWorker() {
		if (this.status != WorkerData.Status.KILLED) {
			ClusterDataManager.updateWorkerData(publicHost, publicPort, WorkerData.Status.KILLED);
			onBeforeBeStopped();
			status = WorkerData.Status.KILLED;
			logger.info("Bye, now exiting " + classnameWorker);
			Utils.exitSystemAfterTimeout(1000);
		}
	}

	final synchronized protected void pauseWorker() {
		status = WorkerData.Status.PAUSED;
		onPause();
	}

	final synchronized protected void restartWorker() {
		if (status == WorkerData.Status.PAUSED) {
			status = WorkerData.Status.RUNNING;
			onRestart();
		} else if (status != WorkerData.Status.RUNNING) {
			startProcessing();
		} else {
			// TODO make sure supervisor is alive, I kill myself because the supervisor will
			// rescue me (reborn)
			// killWorker();
		}
	}

	final protected void registerWorkerNodeIntoCluster() {
		status = WorkerData.Status.STARTED;

		updateClusterInfo();

		// hooking for child
		onStartDone();

		if (autoStart) {
			startProcessing();
		}

		logger.info("started worker Ok at ADDRESS[ " + this.publicHost + ":" + this.publicPort + "] classname:"
				+ classnameWorker);
		Utils.foreverLoop();
	}

	private void updateClusterInfo() {
		try {
			ClusterDataManager.updateWorkerData(publicHost, publicPort, WorkerData.Status.STARTED);

			timer.schedule(new TimerTask() {
				@Override
				public void run() {
					ClusterDataManager.updateWorkerData(publicHost, publicPort, WorkerData.Status.RUNNING);
				}
			}, 5000, 10000);
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
	}

	/**
	 * the handler for common HTTP request to worker
	 * 
	 * @param HttpServerRequest request
	 * @return true if processed | false if no handler found
	 */
	protected final boolean handleRequestToBaseWorker(HttpServerRequest request) {

		final String uri = request.path();
		final HttpServerResponse response = request.response();

		Consumer<HttpServerResponse> handler = BASE_HANDLERS.get(uri);

		if (handler == null) {
			return false;
		}

		handler.accept(response);
		return true;
	}

	private Map<String, Consumer<HttpServerResponse>> createBaseHandlers() {

		Map<String, Consumer<HttpServerResponse>> map = new HashMap<>();

		map.put(URI_PING, res -> res.end("PONG"));

		map.put(URI_PAUSE, res -> {
			pauseWorker();
			res.end("paused");
		});

		map.put(URI_RESTART, res -> {
			restartWorker();
			res.end("restarted");
		});

		map.put(URI_KILL, res -> {
			res.end("Exiting...");
			killWorker();
		});

		map.put(URI_GET_STATUS, res -> res.end(String.valueOf(status)));

		map.put(URI_GET_NAME, res -> res.end(getName()));

		map.put(URI_GET_HOST, res -> res.end(getPublicHost() + ":" + getPublicPort()));

		map.put(URI_GET_SERVER_TIME, res -> res.end(Instant.now().toString()));

		return Map.copyOf(map); // immutable (JDK 11)
	}

	final protected synchronized void startProcessing() {
		if (status != WorkerData.Status.RUNNING) {
			status = WorkerData.Status.RUNNING;
			onProcessing();
		}
	}

	protected void initBeforeStart() {
		logger.info("initBeforeStart " + classnameWorker);
	}

	protected void onStartDone() {
		logger.info("onStartDone " + classnameWorker);
	}

	protected void onProcessing() {
		logger.info("onProcessing " + classnameWorker);
	}

	protected void onRestart() {
		logger.info("onRestart " + classnameWorker);
	}

	protected void onBeforeBeStopped() {
		logger.info("beforeBeKilledByMyself " + classnameWorker);
	}

	protected void onPause() {
		logger.info("onPause " + classnameWorker);
	}

	final public Vertx getVertxInstance() {
		return vertxInstance;
	}

	// for the implementer
	public abstract void start(String host, int port);
}
