package rfx.core.stream.util;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import rfx.core.configs.WorkerConfigs;
import rfx.core.model.WorkerInfo;
import rfx.core.stream.exception.WorkerInfoException;
import rfx.core.stream.message.Tuple;
import rfx.core.stream.model.DataFlowInfo;
import rfx.core.stream.topology.BaseTopology;
import rfx.core.util.CommonUtil;
import rfx.core.util.FileUtils;
import rfx.core.util.StringUtil;
import rfx.core.util.Utils;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Cancellable;
import akka.actor.Props;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class ActorUtil {
	static WorkerConfigs workerConfigs = WorkerConfigs.load();	
	
	public static Config getActorPoolConfig(){
		return ConfigFactory.parseFile(new File(CommonUtil.ACTOR_CONFIG_TEMPLATE_FILE));
	}
	
	public static Config getCmdActorConfig(String host, int port) throws IOException{	
		String s = FileUtils.readFileAsString(CommonUtil.ACTOR_CONFIG_TEMPLATE_FILE);
		s = s.replace("_HostName_", host);
		s = s.replace("_CmdActorPort_", ""+port);
		//System.out.println(s);
		return ConfigFactory.parseString(s);
	}
	
	public static Config getWorkerNodeConfig(WorkerInfo workerInfo) throws IOException{	
		String s = FileUtils.readFileAsString(CommonUtil.ACTOR_CONFIG_TEMPLATE_FILE);
		s = s.replace("_WorkerName_", workerInfo.getName());
		s = s.replace("_HostName_", workerInfo.getHost());
		s = s.replace("_WorkerPort_", ""+workerInfo.getPort());
		//System.out.println(s);
		return ConfigFactory.parseString(s);
	}
	
	public static String buildRemotePath(String workerName, String host, int port){
		return StringUtil.toString("akka.tcp://WorkerNode@" , host , ":" , port , "/user/" , workerName);
	}

	public static int getPortGernaralCommandActor(){
		String[] toks = workerConfigs.getAllocatedCmdActorPortRanges().split("-");
	    int minPort = StringUtil.safeParseInt(toks[0]); 
	    int maxPort = StringUtil.safeParseInt(toks[1]); 
	    int portNumber = minPort + (int)(Math.random() * ((maxPort - minPort) + 1));
	    System.out.println("cmd port:" + portNumber);
	    return portNumber;
	}
	
	public static Map<String, ActorRef> createEmitterPool(BaseTopology topology, Class<?> clazz, List<DataFlowInfo> dataFlowInfos) {
		ActorSystem system = topology.getSystem();
		Map<String, ActorRef> actorPool = new HashMap<>(dataFlowInfos.size());
		for (DataFlowInfo dataFlowInfo : dataFlowInfos) {
			String id = dataFlowInfo.getNamespaceSender() + dataFlowInfo.getUniqueId();		
			Props props = Props.create(clazz,dataFlowInfo, topology);
			ActorRef actor = system.actorOf(props, id);
			actorPool.put(id, actor);
		}
		System.out.println("---createEmitterPool OK for:"+clazz.getName() + ",size:"+actorPool.size());
		return actorPool;
	}	
	
	public static void sendToActor(Map<String, ActorRef> actorPool, String prefixId, Tuple newTuple, ActorRef sender){
		actorPool.get(prefixId + Utils.randomActorId(actorPool.size()-1)).tell(newTuple, sender);
	}
	
	public static void sendToActor(DataFlowInfo dataFlowInfo, Tuple newTuple, ActorRef sender){
		Map<String,Map<String, ActorRef>> receivers = dataFlowInfo.getReceiverActorPools();
		Set<String> namespaceReceivers = receivers.keySet();
		for (String namespaceReceiver : namespaceReceivers) {
			commitSendToActor(dataFlowInfo, newTuple, sender, namespaceReceiver);
		}
	}
	
	public static void sendToActor(DataFlowInfo dataFlowInfo, Tuple newTuple){
		sendToActor(dataFlowInfo, newTuple, ActorRef.noSender());
	}
	
	protected static void commitSendToActor(DataFlowInfo dataFlowInfo, Tuple newTuple, ActorRef sender, String namespaceReceiver) {
		Map<String, ActorRef> actorPool = dataFlowInfo.getReceiverActorPool(namespaceReceiver);
		int rounting = dataFlowInfo.getRoutingStrategy();
		String randomId = "";
		if(rounting == DataFlowInfo.ROUTING_ROUND_ROBIN){
			//randomId = namespaceReceiver + Utils.randomActorId(actorPool.size()-1);
			//TODO
		} else {
			//randomized routing
			randomId = namespaceReceiver + Utils.randomActorId(actorPool.size()-1);
		}
		ActorRef actorRef = actorPool.get(randomId);			
		if(actorRef != null){
			//TODO debug sending flow
			//System.out.println("-----"+dataFlowInfo.getNamespaceSender() + " -> "+namespaceReceiver);
			actorRef.tell(newTuple, sender);				
		} else {
			System.err.println("commitSendToActor failed, no found valid actorRef for id:"+randomId + " ,dataFlowInfo: "+dataFlowInfo);
		}
		randomId = null;
	}
	
	public static void sendToActor(DataFlowInfo dataFlowInfo, Tuple newTuple, ActorRef sender, String namespaceReceiver){		
		if(dataFlowInfo.isValidNamespaceReceiver(namespaceReceiver)){
			commitSendToActor(dataFlowInfo, newTuple, sender, namespaceReceiver);		
		} else {
			//skip if invalid
			newTuple.clear();
		}
	}
	
	public static Map<String, Cancellable> scheduleEmittingData(ActorSystem system, Map<String, ActorRef> dataEmittingActors,long delay, long period, String event){
		Map<String, Cancellable> emitterCancels = new HashMap<String, Cancellable>(dataEmittingActors.size());		
		FiniteDuration delayDuration = Duration.Zero();
		FiniteDuration duration = Duration.create(period, TimeUnit.MILLISECONDS);
		Set<String> actorIds = dataEmittingActors.keySet();
		for (String actorId : actorIds) {
			ActorRef dataEmittingActor = dataEmittingActors.get(actorId);
			if( delay > 0 ){
				delayDuration = Duration.create(delay+10, TimeUnit.MILLISECONDS);
			}
			Cancellable cancellable = system.scheduler().schedule(delayDuration,duration, dataEmittingActor, event, system.dispatcher(), null);
			emitterCancels.put(actorId, cancellable);
			if(delay > 0){
				Utils.sleep(delay);	
			}			
		}
		return emitterCancels;
	}
	
	public static void killAllActorsInPool(ActorSystem system, Map<String, ActorRef> actors){		
		Set<String> actorIds = actors.keySet();
		for (String actorId : actorIds) {
			ActorRef dataEmittingActor = actors.get(actorId);	
			dataEmittingActor.tell(akka.actor.PoisonPill.getInstance(), null);
		}
	}
	
	public static Map<String, Cancellable> scheduleEmittingData(ActorSystem system, Map<String, ActorRef> dataEmittingActors, int seconds, String event){
		return scheduleEmittingData(system, dataEmittingActors, 0, seconds, event);
	}
	
	public static void cancelScheduleEmittingData(Map<String, Cancellable> emitterCancels){
		if(emitterCancels != null){
			Collection<Cancellable> cancellables = emitterCancels.values();
			for (Cancellable cancellable : cancellables) {
				cancellable.cancel();
			}
		}
	}
	
	public static Future<WorkerInfo> checkWorkerInfoByTcpAddress(final ExecutorService es, final WorkerInfo workerInfo, final int timeout) {
		return es.submit(new Callable<WorkerInfo>() {
			@Override
			public WorkerInfo call() {				
				try {
					//System.out.println(" check "+workerInfo.getName());
					Socket socket = new Socket();
					socket.connect(new InetSocketAddress(workerInfo.getHost(), workerInfo.getPort()), timeout);
					socket.close();					
					workerInfo.setAlive(true);
					//System.out.println(" true "+workerInfo.getPort());
					return workerInfo ;
				} catch (Exception ex) {
					workerInfo.setAlive(false);
					//ex.printStackTrace();					
				}				
				return workerInfo;
			}
		});
	}
	
	static List<Future<WorkerInfo>> checkWorkerResource(String workerTypeName){
		String prefixWorkerName = workerConfigs.getPrefixWorkerName();
		String host = workerConfigs.getHostName();
		List<WorkerInfo> allocatedWorkers = workerConfigs.getAllocatedWorkers();
		ExecutorService es = Executors.newFixedThreadPool(allocatedWorkers.size());
		List<Future<WorkerInfo>> futures = new ArrayList<>(allocatedWorkers.size());
		int timeout = 250;		
		for (WorkerInfo w : allocatedWorkers) {
			String name = StringUtil.toString(workerTypeName, prefixWorkerName, "_", host.replaceAll("\\.", ""), "_" , w.getPort());	
			futures.add(ActorUtil.checkWorkerInfoByTcpAddress(es, new WorkerInfo(name, host, w.getPort()), timeout));
		}
		es.shutdown();	
		return futures;
	}
	
	public static List<WorkerInfo> getFreeWorkerResources(String workerTypeName) {
		List<Future<WorkerInfo>> futures = checkWorkerResource(workerTypeName);
		List<WorkerInfo> freeworkerInfos = new ArrayList<>(futures.size());
		for (final Future<WorkerInfo> f : futures) {
			try {
				WorkerInfo checkedWorkerInfo = f.get();
				if ( ! checkedWorkerInfo.isAlive()) {
					//not alive means free resource for system (no one is using a TCP port)
					freeworkerInfos.add(checkedWorkerInfo);					
				}
			}  catch (Exception e) {
				e.printStackTrace();
			}
		}
		return freeworkerInfos;
	}
	
	public static WorkerInfo allocateWorkerInfo(String workerTypeName) throws WorkerInfoException{
		List<Future<WorkerInfo>> futures = checkWorkerResource(workerTypeName);	
		for (final Future<WorkerInfo> f : futures) {
			try {
				WorkerInfo checkedWorkerInfo = f.get();
				if ( ! checkedWorkerInfo.isAlive()) {
					return checkedWorkerInfo;
				}
			}  catch (Exception e) {
				e.printStackTrace();
			}
		}
		String prefixWorkerName = workerConfigs.getPrefixWorkerName();
		String host = workerConfigs.getHostName();
		List<WorkerInfo> tcpPorts = workerConfigs.getAllocatedWorkers();
		throw new WorkerInfoException("No TCP Port available for "+prefixWorkerName +" at "+ host + " in " + tcpPorts );
	}
	
	public static Map<String, ActorRef> createActorPool(ActorSystem system, int maxPoolSize, Class<?> clazz, DataFlowInfo dataFlowInfo){
		//TODO
		return null;
	}
}

