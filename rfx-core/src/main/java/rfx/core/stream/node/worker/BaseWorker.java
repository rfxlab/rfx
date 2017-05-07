package rfx.core.stream.node.worker;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import com.google.gson.Gson;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetSocket;
import redis.clients.jedis.ShardedJedisPool;
import redis.clients.jedis.exceptions.JedisException;
import rfx.core.model.WorkerTimeLog;
import rfx.core.nosql.jedis.RedisCommand;
import rfx.core.stream.cluster.ClusterDataManager;
import rfx.core.util.StringPool;
import rfx.core.util.StringUtil;
import rfx.core.util.Utils;

/**
 * the base class for worker implementation <br>
 * a worker could be a scheduled job, a kafka stream processor or a http web service
 * 
 * @author trieunt <br>
 *
 */
public abstract class BaseWorker {
	
	public final long MAX_TIMEOUT_WORKER = 10000000000L;
	private static final String URI_GET_SERVER_TIME = "/get/server-time";
	private static final String URI_GET_HOST = "/get/host";
	private static final String URI_GET_NAME = "/get/name";
	private static final String URI_GET_STATUS = "/get/status";
	private static final String URI_KILL = "/kill";
	private static final String URI_RESTART = "/restart";
	private static final String URI_PAUSE = "/pause";
	private static final String URI_PING = "/ping";
	public final int STARTING = 0;
	public final int STARTED = 1;
	public final int RUNNING = 2;
	public final int PAUSED = 3;
	public final int KILLED = 4;
	
	
	static BaseWorker _worker;
	
	protected String publicHost;
	protected int publicPort;
	protected String privateHost;
	protected int privatePort;
	protected String name;
	protected String classnameWorker = getClass().getName();
	protected int status = -1;
	protected boolean autoStart = true;
	protected Vertx vertxInstance;
	protected HttpServer httpServerInstance;
	
	protected Timer timer = new Timer(true);
	
	public static BaseWorker getInstance(){
		return _worker;
	}
	
	protected static void setWorker(BaseWorker worker){
		_worker = worker;
	}		
	
	
	public BaseWorker(String name) {
		super();
		
		this.name = name;
		this.classnameWorker = getClass().getName();
		status = STARTING;
		initBeforeStart();
	}
	
	public BaseWorker(String name, boolean autoStart) {
		this(name);
		this.autoStart = autoStart;
	}

	final public String getName() {
		return name;
	}
	
	
	final public int getStatus() {
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
	
	final protected boolean isAddressAlreadyInUse(String host, int port){
		int timeout = 500;
		try {
			//System.out.println(" check "+workerInfo.getName());
			Socket socket = new Socket();
			socket.connect(new InetSocketAddress(host, port), timeout);
			socket.close();					
			return true;
		} catch (Exception ex) {								
		}			
		return false;
	}
	
	private HttpServer checkAndCreateHttpServer(String host, int port){
		if(isAddressAlreadyInUse(host, port)){
			System.err.println(host+":"+port + " isAddressAlreadyInUse!");
			Utils.exitSystemAfterTimeout(200);
			return null;
		}
		try {
			this.publicHost = host;
			this.publicPort = port;
			
			// disable the creation of file-cache folders ".vertx"
			System.setProperty("vertx.disableFileCPResolving", "true");
			
			//refer http://vertx.io/manual.html#performance-tuning
			//DeploymentOptions options = new DeploymentOptions().setWorker(true);
			VertxOptions options = new VertxOptions(); 
			options.setMaxEventLoopExecuteTime(MAX_TIMEOUT_WORKER);			
			vertxInstance = Vertx.vertx(options);
			
			HttpServerOptions httpOptions = new HttpServerOptions();
			httpOptions.setAcceptBacklog(10000).setUsePooledBuffers(true);
			httpOptions.setSendBufferSize(4 * 1024);
			httpOptions.setReceiveBufferSize(4 * 1024);
			httpServerInstance = vertxInstance.createHttpServer(httpOptions);
			
			return httpServerInstance;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	private NetServer checkAndCreateNetServer(String host, int port){
		if(isAddressAlreadyInUse(host, port)){
			System.err.println(host+":"+port + " isAddressAlreadyInUse!");
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
		if(server == null){
			System.err.println("registerWorkerHttpHandler return NULL value");
			return;
		}
		server.requestHandler(handler).listen(port, host);
		registerWorkerNodeIntoCluster();		
	}
	
	
	final protected void registerWorkerTcpHandler(String host, int port, Handler<NetSocket> handler) {
		NetServer server = checkAndCreateNetServer(host, port);
		if(server == null){
			System.err.println("checkAndCreateNetServer return NULL value");
			return;
		}
		System.out.println(String.format("...registerWorkerTcpHandler %s:%s",host,port));
		server.connectHandler(handler).listen(port, host);
		//TODO
		//registerWorkerNodeIntoCluster();		
	}
	
	final synchronized protected void killWorker(){
		if(this.status != KILLED){
	        ShardedJedisPool jedisPool =  ClusterDataManager.getRedisClusterInfoPool();
	        new RedisCommand<Boolean>(jedisPool) {
	            @Override
	            protected Boolean build() throws JedisException {	                
	                String workerName = StringUtil.toString(publicHost.replaceAll("\\.", ""), "_", publicPort);
	                WorkerTimeLog timeLog = new Gson().fromJson(jedis.hget(ClusterDataManager.CLUSTER_WORKER_PREFIX, workerName + ClusterDataManager.WORKER_TIMELOG_POSTFIX), WorkerTimeLog.class);
	                if (timeLog == null) {
	                    timeLog = new WorkerTimeLog();
	                }
	                timeLog.addDownTime(System.currentTimeMillis());
	                jedis.hset(ClusterDataManager.CLUSTER_WORKER_PREFIX, workerName + ClusterDataManager.WORKER_TIMELOG_POSTFIX, new Gson().toJson(timeLog));
	                return true;
	            }
	        }.execute();
	        onBeforeBeStopped();			
			status = KILLED;
			System.out.println("Bye, now exiting "+classnameWorker);
			Utils.exitSystemAfterTimeout(1000);	
		}
	}
	
	final synchronized protected void pauseWorker(){
		status = PAUSED;
		onPause();
	}
	
	final synchronized protected void restartWorker(){
		if(status == PAUSED){
			status = RUNNING;
			onRestart();
		} else if (status != RUNNING){
			startProcessing();
		} else {
			//TODO make sure supervisor is alive, I kill myself because the supervisor will rescue me (reborn)			
			//killWorker();
		}
	}
	
	final protected void registerWorkerNodeIntoCluster() {		
		status = STARTED;		
		
		updateClusterInfo();
        
        //hooking for child
		onStartDone();
		
		if(autoStart){
			startProcessing();			
		}
		
		System.out.println("started worker Ok at ADDRESS[ "+this.publicHost + ":" + this.publicPort + "] classname:" +classnameWorker);
		Utils.foreverLoop();
	}

	private void updateClusterInfo() {
		try {
			ShardedJedisPool jedisPool =  ClusterDataManager.getRedisClusterInfoPool();
			new RedisCommand<Boolean>(jedisPool) {
			    @Override
			    protected Boolean build() throws JedisException {
			        jedis = shardedJedis.getShard(StringPool.BLANK);
			        String workerName = StringUtil.toString(publicHost.replaceAll("\\.", ""), "_", publicPort);
			        WorkerTimeLog timeLog = new Gson().fromJson(jedis.hget(ClusterDataManager.CLUSTER_WORKER_PREFIX, workerName + ClusterDataManager.WORKER_TIMELOG_POSTFIX), WorkerTimeLog.class);
			        if (timeLog == null) {
			            timeLog = new WorkerTimeLog();
			        }
			        timeLog.addUpTime(System.currentTimeMillis());
			        jedis.hset(ClusterDataManager.CLUSTER_WORKER_PREFIX, workerName + ClusterDataManager.WORKER_TIMELOG_POSTFIX, new Gson().toJson(timeLog));
			        return true;
			    }
			}.execute();
			
			timer.schedule(new TimerTask() {
			    @Override
			    public void run() {
			        ClusterDataManager.updateWorkerData(publicHost, publicPort);
			    }
			}, 2000, 2000);
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
	final protected boolean handleRequestToBaseWorker(HttpServerRequest request){
		String uri = request.path();
		HttpServerResponse res = request.response();
		if (uri.equalsIgnoreCase(URI_PING)) {
        	res.end("PONG");
            return true;
        } else if (uri.equalsIgnoreCase(URI_PAUSE)) {
        	pauseWorker();
			res.end("paused");
			return true;
        } else if (uri.equalsIgnoreCase(URI_RESTART)) {
        	restartWorker();
			res.end("restarted");
			return true;
        } else if (uri.equalsIgnoreCase(URI_KILL)) {
			res.end("Exiting...");
            killWorker();
            return true;
        } else if (uri.equalsIgnoreCase(URI_GET_STATUS)) {        	
        	res.end(""+status);
        	return true;
        } else if (uri.equalsIgnoreCase(URI_GET_NAME)) {        	
        	res.end(getName());
        	return true;
        } else if (uri.equalsIgnoreCase(URI_GET_HOST)) {        	
        	res.end(getPublicHost()+":"+getPublicPort());
        	return true;
        } else if (uri.equalsIgnoreCase(URI_GET_SERVER_TIME)) {        	
        	res.end(new Date().toString());
        	return true;
        }
		return false;
	}
	
	final protected synchronized void startProcessing(){
		if(status != RUNNING){
			status = RUNNING;
			onProcessing();
		}
	}	
	
	// for the implementer
	public abstract void start(String host, int port);
	
	protected void initBeforeStart(){
		System.out.println("initBeforeStart "+classnameWorker);
	}
	
	protected void onStartDone() {
		System.out.println("onStartDone "+classnameWorker);
	}
	
	protected void onProcessing() {
		System.out.println("onProcessing "+classnameWorker);
	}
	
	protected void onRestart() {
		System.out.println("onRestart "+classnameWorker);
	}
	
	
	protected void onBeforeBeStopped() {		
		System.out.println("beforeBeKilledByMyself "+classnameWorker);
	}
	
	protected void onPause() {
		System.out.println("onPause "+classnameWorker);
	}	
	
	final public Vertx getVertxInstance() {
		return vertxInstance;
	}
}
