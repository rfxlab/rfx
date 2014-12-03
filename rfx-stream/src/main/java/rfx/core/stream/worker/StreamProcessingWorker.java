package rfx.core.stream.worker;

import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.HttpServerResponse;

import rfx.core.configs.loader.ConfigAutoLoader;
import rfx.core.stream.model.KafkaTaskDef;
import rfx.core.stream.model.TaskDef;
import rfx.core.stream.node.worker.BaseWorker;
import rfx.core.stream.topology.BaseTopology;
import rfx.core.util.LogUtil;
import rfx.core.util.StringUtil;

import com.google.gson.Gson;

public abstract class StreamProcessingWorker extends BaseWorker {
	
	protected TaskDef taskDef;
	static final String KAFKA_TOPOLOGY_CONFIGS_FILE = "configs/kafka-topology-configs.xml"; 
	
	public StreamProcessingWorker(String name) {
		super(name);		
		LogUtil.setPrefixFileName(name);
		ConfigAutoLoader.loadAll();
		ConfigAutoLoader.load(KAFKA_TOPOLOGY_CONFIGS_FILE);
	}
	
	public StreamProcessingWorker(String name, boolean autoStart) {
		this(name);
		this.autoStart = autoStart;
	}
	
	final protected boolean handleRequestDone(HttpServerRequest req){
		if (handleRequestToBaseWorker(req)) {
            return true;
        }
		String uri = req.absoluteURI().getPath();
		HttpServerResponse res = req.response();
		MultiMap params = req.params();
		if (uri.equals("/set/task")) {
			String type = params.get("type");
			if("KafkaTaskDef".equals(type)){
				String topic = params.get("topic");
				String classPath = params.get("classpath");
				int beginPartitionId = StringUtil.safeParseInt(params.get("begin-partition"));
				int endPartitionId = StringUtil.safeParseInt(params.get("end-partition"));
				taskDef = new KafkaTaskDef(topic, classPath, beginPartitionId, endPartitionId);
				
				if(taskDef.isAutoStart()){
					startProcessing();
				}
				
				res.end("set-task-ok:"+new Gson().toJson(taskDef));
				return true;
			}
        }
		return false;
	}
	
	final public StreamProcessingWorker setTaskDef(TaskDef taskDef) {
		this.taskDef = taskDef;
		return this;
	}
	
	@Override
    final public void start(String host, int port) {
        Handler<HttpServerRequest> handler = new Handler<HttpServerRequest>() {
            public void handle(HttpServerRequest request) {
            	if(handleRequestDone(request)){
            		return;
            	}
                request.response().end("No HTTP handler found in worker-class:"+getName());
            }
        };
        registerWorkerHttpHandler(host, port, handler);
    }
	
	 @Override
	 final protected void onStartDone() {		
		System.out.println("Ready to processing stream data!");
				
		//TODO load previous tasks from Redis and set to taskDef
	 }
	 
	 @Override
	 final protected void onProcessing() {
		 if(taskDef != null){
			 if(taskDef instanceof KafkaTaskDef){
				KafkaTaskDef kafkaTaskDef = (KafkaTaskDef) taskDef;
				String kafkaTopic = kafkaTaskDef.getTopic();
				int beginPartitionId = kafkaTaskDef.getBeginPartitionId();
				int endPartitionId = kafkaTaskDef.getEndPartitionId();
				BaseTopology topology = null;
				if( StringUtil.isNotEmpty( kafkaTaskDef.getClassPath()) ){						
					topology = BaseTopology.loadTopologyFromClasspath(kafkaTopic, kafkaTaskDef.getClassPath());
				} else {
					topology = BaseTopology.lookupTopologyFromTopic(kafkaTopic);
				}					
				if(topology != null){
					topology.initKafkaDataSeeders(kafkaTopic, beginPartitionId, endPartitionId).buildTopology().start();
				}							
			 }
		 }		
	}
	
}
