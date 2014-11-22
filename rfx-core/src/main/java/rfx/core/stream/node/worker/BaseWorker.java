package rfx.core.stream.node.worker;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.VertxFactory;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.HttpServerResponse;
import org.vertx.java.core.http.RouteMatcher;

import redis.clients.jedis.ShardedJedisPool;
import redis.clients.jedis.exceptions.JedisException;
import rfx.core.model.WorkerTimeLog;
import rfx.core.nosql.jedis.RedisCommand;
import rfx.core.stream.cluster.ClusterDataManager;
import rfx.core.util.StringPool;
import rfx.core.util.StringUtil;
import rfx.core.util.Utils;

import com.google.gson.Gson;

/**
 * the base class for worker implementation <br>
 * a worker could be a scheduled job, a kafka stream processor or a http web service
 * 
 * @author trieunt <br>
 *
 */
public abstract class BaseWorker {
	
	public final int STARTING = 0;
	public final int STARTED = 1;
	public final int RUNNING = 2;
	public final int PAUSED = 3;
	public final int KILLED = 4;
	
	static BaseWorker _worker;
	
	protected String host;
	protected int port;
	protected String name;
	protected String classnameWorker = getClass().getName();
	int status = -1;
	boolean autoStart = true;
	
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

	final public String getName() {
		return name;
	}
	
	
	final public int getStatus() {
		return status;
	}
	
	final public String getHost() {
		return host;
	}

	final public int getPort() {
		return port;
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
			this.host = host;
			this.port = port;
			Vertx vertx = VertxFactory.newVertx();    
			return vertx.createHttpServer();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	final protected void registerWorkerHttpHandler(String host, int port, Handler<HttpServerRequest> handler) {
		HttpServer server = checkAndCreateHttpServer(host, port);
		if(server == null){
			return;
		}
		server.requestHandler(handler).listen(port, host);
		registerWorkerNodeIntoCluster();		
	}
	
	final protected void registerWorkerHttpHandler(String host, int port, RouteMatcher routeMatcher) {
		HttpServer server = checkAndCreateHttpServer(host, port);
		if(server == null){
			return;
		}
		server.requestHandler(routeMatcher).listen(port, host);
		registerWorkerNodeIntoCluster();		
	}
	
	final synchronized protected void killWorker(){
		if(this.status != KILLED){
	        ShardedJedisPool jedisPool =  ClusterDataManager.getRedisClusterInfoPool();
	        new RedisCommand<Boolean>(jedisPool) {
	            @Override
	            protected Boolean build() throws JedisException {
	                jedis = shardedJedis.getShard(StringPool.BLANK);
	                String workerName = StringUtil.toString(host.replaceAll("\\.", ""), "_", port);
	                WorkerTimeLog timeLog = new Gson().fromJson(jedis.hget(ClusterDataManager.CLUSTER_WORKER_PREFIX, workerName + ClusterDataManager.WORKER_TIMELOG_POSTFIX), WorkerTimeLog.class);
	                if (timeLog == null) {
	                    timeLog = new WorkerTimeLog();
	                }
	                timeLog.addDownTime(System.currentTimeMillis());
	                jedis.hset(ClusterDataManager.CLUSTER_WORKER_PREFIX, workerName + ClusterDataManager.WORKER_TIMELOG_POSTFIX, new Gson().toJson(timeLog));
	                return true;
	            }
	        }.execute();
	        beforeBeKilledByMyself();			
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
		
		ShardedJedisPool jedisPool =  ClusterDataManager.getRedisClusterInfoPool();
        new RedisCommand<Boolean>(jedisPool) {
            @Override
            protected Boolean build() throws JedisException {
                jedis = shardedJedis.getShard(StringPool.BLANK);
                String workerName = StringUtil.toString(host.replaceAll("\\.", ""), "_", port);
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
                ClusterDataManager.updateWorkerData(host, port);
            }
        }, 2000, 2000);
        
        //hooking for child
		onStartDone();
		
		if(autoStart){
			startProcessing();			
		}
		
		System.out.println("started worker Ok at ADDRESS[ "+this.host + ":" + this.port + "] classname:" +classnameWorker);
		Utils.foreverLoop();
	}
	
	/**
	 * the handler for common HTTP request to worker
	 * 
	 * @param HttpServerRequest request
	 * @return true if processed | false if no handler found
	 */
	final protected boolean handleRequestToBaseWorker(HttpServerRequest request){
		String uri = request.absoluteURI().getPath();
		HttpServerResponse res = request.response();
		if (uri.equalsIgnoreCase("/ping")) {
        	res.end("PONG");
            return true;
        } else if (uri.equalsIgnoreCase("/pause")) {
        	pauseWorker();
			res.end("paused");
			return true;
        } else if (uri.equalsIgnoreCase("/restart")) {
        	restartWorker();
			res.end("restarted");
			return true;
        } else if (uri.equalsIgnoreCase("/kill")) {
			res.end("Exiting...");
            killWorker();
            return true;
        } else if (uri.equalsIgnoreCase("/get/status")) {        	
        	res.end(""+status);
        	return true;
        } else if (uri.equalsIgnoreCase("/get/name")) {        	
        	res.end(getName());
        	return true;
        } else if (uri.equalsIgnoreCase("/get/host")) {        	
        	res.end(getHost()+":"+getPort());
        	return true;
        } else if (uri.equalsIgnoreCase("/get/server-time")) {        	
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
	
	
	protected void beforeBeKilledByMyself() {		
		System.out.println("beforeBeKilledByMyself "+classnameWorker);
	}
	
	protected void onPause() {
		System.out.println("onPause "+classnameWorker);
	}	
}
