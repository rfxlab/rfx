package rfx.core.stream.topology;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import rfx.core.configs.loader.ConfigAutoLoader;
import rfx.core.model.Callback;
import rfx.core.model.CallbackResult;
import rfx.core.stream.configs.ActorConfigs;
import rfx.core.stream.configs.KafkaTopologyConfig;
import rfx.core.stream.emitters.DataEmitter;
import rfx.core.stream.emitters.DataFileEmitter;
import rfx.core.stream.emitters.EmittedDataListener;
import rfx.core.stream.functor.common.DataFileSourceFunctor;
import rfx.core.stream.functor.common.DataSourceFunctor;
import rfx.core.stream.functor.common.KafkaDataSourceFunctor;
import rfx.core.stream.kafka.KafkaDataEmitter;
import rfx.core.stream.kafka.KafkaDataSeeder;
import rfx.core.stream.model.DataFlowInfo;
import rfx.core.stream.model.DataFlowPostProcessing;
import rfx.core.stream.util.ActorUtil;
import rfx.core.util.LogUtil;
import rfx.core.util.Utils;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Cancellable;
import akka.actor.Props;

import com.typesafe.config.Config;

/**
 * The BaseTopology for distributed processing
 * 
 * @author trieu
 *
 */
public abstract class BaseTopology {

	private static final String CONFIGS_KAFKA_TOPOLOGY_CONFIGS_XML = "configs/kafka-topology-configs.xml";

	static Config actorConfig = ActorConfigs.getDefault(); 
	
	protected ActorSystem system;
	protected String topologyName;
	private int totalActors = 0;
	
	//common for data emitters	
	protected List<KafkaDataSeeder> kafkaLogSeeders = new ArrayList<>();
	protected Map<String,DataEmitter> eventStreamEmitters = new HashMap<>();	
	protected Map<String, ActorRef> emittingDataActors;
	protected Map<String, Cancellable> emitterCancels;
	protected EmittedDataListener emittedDataListener = new EmittedDataListener() {		
		@Override
		public void processingDone() {
			System.out.println("Processing done!");
		}
	};
	
	
	//control how system will shutdown safely
	protected volatile boolean shutdownSystem = false;
	
	//logging metrics to redis
	private Map<String, AtomicLong> counters = new ConcurrentHashMap<String, AtomicLong>();
	private static Timer metricsTimer = new Timer(true);
	
	//the topology flow of system 
	protected Map<Integer, DataFlowInfo> topologyFlows = new TreeMap<>();
	
	public BaseTopology() {
		super();		
		init(getClass().getSimpleName());
	}
		
	public BaseTopology(String topologyName) {
		super();
		init(topologyName);
	}
	
	private void init(String topologyName){		
		ConfigAutoLoader.load( CONFIGS_KAFKA_TOPOLOGY_CONFIGS_XML );
		this.topologyName = topologyName;		
		system = ActorSystem.create(this.topologyName,actorConfig);		
	}
	
	public BaseTopology addDataFile(String path){
		Queue<String> dataFiles = new LinkedList<String>();
		dataFiles.add(path);
		return addDataFileEmitter(new DataFileEmitter(dataFiles));
	}
	
	public BaseTopology addDataFile(String keyEmitter, String path){
		Queue<String> dataFiles = new LinkedList<String>();
		dataFiles.add(path);
		DataEmitter emitter = eventStreamEmitters.get(keyEmitter);
		if(emitter != null){
			if(emitter instanceof DataFileEmitter){
				((DataFileEmitter)emitter).addFile(path);
			}
		} else {
			emitter = new DataFileEmitter(dataFiles);
			eventStreamEmitters.put(keyEmitter,emitter);
		}
		return this;
	}
	
	public BaseTopology addDataFiles(Queue<String> dataFiles){		
		return addDataFileEmitter(new DataFileEmitter(dataFiles));
	}
	
	public BaseTopology addDataFiles(String ... paths){
		Queue<String> dataFiles = new LinkedList<String>();
		for (String path : paths) {
			dataFiles.add(path);
		}		
		return addDataFileEmitter(new DataFileEmitter(dataFiles));
	}
	
	public BaseTopology addDataFileEmitter(DataFileEmitter emitter){
		String k = "DataFileEmitter"+(eventStreamEmitters.size()+1);
		eventStreamEmitters.put(k,emitter);
		return this;
	}
	
	public int getDataEmitterSize(){
		return this.eventStreamEmitters.size();
	}
	
	public BaseTopology initKafkaDataSeeders(String topic, int beginPartitionId, int endPartitionId){
		List<KafkaDataSeeder> dataSeeders = new ArrayList<>(); 
		for (int partition = beginPartitionId; partition <= endPartitionId; partition++) {
			dataSeeders.add(new KafkaDataSeeder(topic, partition));
		}	
		return setKafkaDataSeeders(dataSeeders);
	}
	
	public BaseTopology setKafkaDataSeeders(List<KafkaDataSeeder> dataSeeders) {
		if(this.kafkaLogSeeders == null){
			this.kafkaLogSeeders = dataSeeders;
		} else {
			this.kafkaLogSeeders.addAll(dataSeeders);
		}
		return this;
	}

	public void shutdownTopologyActorSystem(){
		shutdownSystem = true;
		metricsTimer.cancel();
		cancelScheduleEmittingData();		
		ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
		executor.schedule(new Runnable() {			
			@Override
			public void run() {				 
				system.shutdown();	
				//shutdown other services
				LogUtil.shutdownLogThreadPools();				
				System.out.println("Shutdown actor system OK! BYE BYE ...");
				Utils.exitSystemAfterTimeout(1000);
			}
		},4,TimeUnit.SECONDS);
		executor.shutdown();
	}
	
	public synchronized AtomicLong counter() {		
		return counter(topologyName);
	}
	
	public ActorSystem getSystem() {
		return system;
	}
	
	public Map<Integer, DataFlowInfo> getTopologyFlows() {
		return topologyFlows;
	}
	
	public synchronized AtomicLong counter(String metricName) {
		AtomicLong counter = counters.get(metricName);
		if(counter == null){
			counter = new AtomicLong(0);
			counters.put(metricName, counter);
		}
		return counter;
	}
	
	public List<DataEmitter> getDataEmitters() {
		return new ArrayList<>(eventStreamEmitters.values());
	}
	
	public BaseTopology addDataEmitter(DataEmitter emitter) {
		String k = emitter.getClass().getName()+(eventStreamEmitters.size()+1);
		eventStreamEmitters.put(k,emitter);
		return this;
	}
	
	public Map<String, ActorRef> getEmittingDataActors() {
		return emittingDataActors;
	}	
	
	private List<DataFlowInfo> getEmitFlows(Class<?> clazzSender){
		List<DataFlowInfo> emitflows = new ArrayList<>(eventStreamEmitters.size());
		//emit log actors		
		int i = 0;
		for (DataEmitter dataEmitter : eventStreamEmitters.values()) {
			i++;
			DataFlowInfo emitflowInfo = new DataFlowInfo(clazzSender, String.valueOf(i));
			emitflowInfo.setPreProcessingFunction(dataEmitter);
			emitflowInfo.setPostProcessingFunction(new DataFlowPostProcessing() {				
				@Override
				public CallbackResult<String> call() {
					counter("DataEmitter").incrementAndGet();
					return null;
				}
			});
			emitflows.add(emitflowInfo);
		}
		return emitflows;
	}
		
	private List<DataFlowInfo> defineDataEmitter(Class<?> clazzSender){
		//FIXME better way ??
		List<DataFlowInfo> emitflows = null;
		if(this.kafkaLogSeeders.size()>0){
			int i = 1;
			for (KafkaDataSeeder kafkaDataSeeder : kafkaLogSeeders) {
				DataEmitter dataEmitter = new KafkaDataEmitter(kafkaDataSeeder);
				eventStreamEmitters.put("KafkaDataEmitter"+i,dataEmitter);
				i++;
			}
			emitflows = getEmitFlows(clazzSender);
			this.emittingDataActors = ActorUtil.createEmitterPool(this, KafkaDataSourceFunctor.class, emitflows);
			return emitflows;
		}		
		
		//default, read from files
		emitflows = getEmitFlows(clazzSender);
		this.emittingDataActors = ActorUtil.createEmitterPool(this, DataFileSourceFunctor.class, emitflows);
		return emitflows;
	}
	
	public void setReceiverFromEmitter( Class<?> clazzReceiver, Map<String, ActorRef> poolReceiver){		
		List<DataFlowInfo> emitflows = defineDataEmitter(DataEmitter.class);
		if(emitflows != null){
			for (DataFlowInfo dataFlowInfo : emitflows) {
				dataFlowInfo.addReceiverActorPool(clazzReceiver, poolReceiver);	
			}
		}	
	}	
	
	public BaseTopology scheduleEmittingData(long period){		
		return scheduleEmittingData(500, period);
	}
	
	public BaseTopology scheduleEmittingData(long delay, long period){
		if(emittingDataActors == null){
			throw new IllegalStateException("Please calling defineDataEmitter before calling scheduleEmittingData");
		}
		if(shutdownSystem){
			return this;
		} else {
			String emitLogEvent = DataSourceFunctor.EMIT_LOG_EVENT;
			this.emitterCancels = ActorUtil.scheduleEmittingData(system, emittingDataActors, delay, period, emitLogEvent);
		}
		metricsTimer.schedule(new TimerTask() {			
			@Override
			public void run() {
				//ClusterDataManager.logMetrics(counters);
				//TODO
			}
		}, 1000, 5000);
		return this;
	}
	
	public int cancelScheduleEmittingData(){
		if(emitterCancels != null){
			ActorUtil.cancelScheduleEmittingData(this.emitterCancels);
			return emitterCancels.size();
		}		
		if(emittingDataActors != null){
			ActorUtil.killAllActorsInPool(system, emittingDataActors);
		}
		return 0;
	}
	
	public boolean isCounterOverLimit(long limit){
		return this.counter().longValue() >= limit;
	}
	
	public boolean whenCounterOverLimit(long limit, Callback<String> callback){
		boolean rs = this.counter().longValue() >= limit;
		if(rs && callback != null){
			callback.call();
		}
		return rs;
	}
	
	
	public Map<String, ActorRef> createActorPool( Class<?> clazz, DataFlowInfo dataFlowInfo, int maxPoolSize) {
		Map<String, ActorRef> actorPool = new HashMap<>(maxPoolSize);
		for (int i = 0; i < maxPoolSize; i++) {
			String id = dataFlowInfo.getNamespaceSender() + i;		
			Props props = Props.create(clazz,dataFlowInfo,this);
			ActorRef actor = system.actorOf(props, id);
			actorPool.put(id, actor);
		}		
		//keep the ordering of flows
		topologyFlows.put(topologyFlows.size()+1, dataFlowInfo);
		totalActors+=maxPoolSize;
		System.out.println("---createActorPool OK for:"+clazz.getName() + ",size:"+actorPool.size());
		return actorPool;
	}
	
	protected Map<String, ActorRef> createActorPool(Class<?> clazz, int maxPoolSize) {
		return createActorPool(clazz, new DataFlowInfo(clazz), maxPoolSize);
	}

	public String getTopologyFlowsAsString() {
		StringBuilder s = new StringBuilder();
		Collection<DataFlowInfo> flows = this.topologyFlows.values();
		for (DataFlowInfo flow : flows) {
			s.append(flow.toString()).append(" ");
		}
		return s.toString().trim();
	}
	
	public int getTotalActors() {
		return totalActors;
	}
	
	public static BaseTopology loadTopologyFromClasspath(String topic, String classpath){
		try {
			@SuppressWarnings("unchecked")
			Class<BaseTopology> clazz = (Class<BaseTopology>) Class.forName(classpath);
			BaseTopology topology = clazz.newInstance();
			return topology;
		}  catch (Exception e) {
			if (e instanceof ClassNotFoundException) {
				System.err.println(e.getClass().getName() + " for classpath:" + classpath + " by topic:" + topic);
			} 
			LogUtil.error(e);
		}
		return null;
	}

	public static BaseTopology lookupTopologyFromTopic(String topic){		
		// lookup the classpath for the topic
		String classpath = KafkaTopologyConfig.getTopologyClassPath(topic);
		if(classpath != null){
			System.out.println("For topic:" + topic + " , begin create topology: "	+ classpath);					
			return loadTopologyFromClasspath(topic, classpath);
		}
		return null;
	}
	
	protected void defaultShutdownKafkaWorker(){
		if(shutdownSystem){
			return;
		}

		//stop seeding data
		KafkaDataSeeder.stopSeedingAndWait(kafkaLogSeeders);
		
		//shutdown actor system after 5 seconds
		this.shutdownTopologyActorSystem();		
	}
	
	
	public EmittedDataListener getEmittedDataListener() {
		return emittedDataListener;
	}

	public void setEmittedDataListener(EmittedDataListener emittedDataListener) {
		this.emittedDataListener = emittedDataListener;
	}

	public void shutdownAll(){
		this.defaultShutdownKafkaWorker();
	}
	
	public void start(){
		this.scheduleEmittingData(1600);
	}
	
	public void start(long period){
		this.scheduleEmittingData(period);
	}
	
	//abstract class for a implementation
	public abstract BaseTopology buildTopology();
}
