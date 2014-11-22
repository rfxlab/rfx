package rfx.core.stream.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import rfx.core.model.Callback;
import rfx.core.util.StringUtil;
import akka.actor.ActorRef;

public class DataFlowInfo {
	
	public static final int ROUTING_RANDOMIZED = 1;
	public static final int ROUTING_ROUND_ROBIN = 2;
	
	private Map<String,Class<?>> clazzReceivers = new HashMap<>();
	private Map<String,Map<String, ActorRef>> receiverActorPools = new HashMap<>();
	
	Class<?> clazzSender;	
	private String namespaceSender;
	
	private String uniqueId;	
	private int routingStrategy = ROUTING_RANDOMIZED;
	
	Callback postProcessingFunction;
	Callback preProcessingFunction;	
	
	public DataFlowInfo(Class<?> clazzSender) {
		super();
		this.clazzSender = clazzSender;
		this.namespaceSender = clazzSender.getSimpleName();
	}
	
	public DataFlowInfo(Class<?> clazzSender, String uniqueId) {
		super();
		this.clazzSender = clazzSender;
		this.namespaceSender = clazzSender.getSimpleName();
		this.uniqueId = uniqueId;
	}

	
	public String getNamespaceSender() {
		if(namespaceSender == null){
			namespaceSender = "AnonymousSender";
		}
		return namespaceSender;
	}

	public void setNamespaceSender(String namespaceSender) {
		this.namespaceSender = namespaceSender;
	}

	public Map<String,Map<String, ActorRef>> getReceiverActorPools() {		
		return receiverActorPools;
	}
	
	public Map<String, ActorRef> getReceiverActorPool(String namespace) {		
		return receiverActorPools.get(namespace);
	}
	
	public int getReceiverActorPoolSize(){
		return getReceiverActorPools().size();
	}	
	
	public int getRoutingStrategy() {
		return routingStrategy;
	}

	public void setRoutingStrategy(int routingStrategy) {
		this.routingStrategy = routingStrategy;
	}

	public Class<?> getClazzSender() {
		return clazzSender;
	}

	public Map<String, Class<?>> getClazzReceivers() {
		return clazzReceivers;
	}

	public String getUniqueId() {
		return uniqueId;
	}


	public DataFlowPostProcessing getPostProcessingFunction() {
		return (DataFlowPostProcessing) postProcessingFunction;
	}

	public void setPostProcessingFunction(DataFlowPostProcessing callback) {
		this.postProcessingFunction = callback;
	}

	public DataFlowPreProcessing getPreProcessingFunction() {
		return (DataFlowPreProcessing) preProcessingFunction;
	}

	public void setPreProcessingFunction(DataFlowPreProcessing callback) {
		this.preProcessingFunction = callback;
	}
	
	public void addReceiverActorPool(Class<?> clazzReceiver, Map<String, ActorRef> receiverActorPool){
		String namespaceReceiver = clazzReceiver.getSimpleName();
		if(receiverActorPool != null){
			if(receiverActorPool.size()>0){
				this.clazzReceivers.put(namespaceReceiver, clazzReceiver);
				this.receiverActorPools.put(namespaceReceiver, receiverActorPool);
			} else {
				throw new IllegalArgumentException("receiverActorPool.size MUST > 0 !");
			}
		} else {
			throw new IllegalArgumentException("receiverActorPool and namespaceReceiver MUST NOT NULL !");
		}
	}
	
	public boolean isValidNamespaceReceiver(String namespaceReceiver){
		return this.receiverActorPools.containsKey(namespaceReceiver);
	}	
	
	@Override
	public String toString() {
		StringBuilder s = new StringBuilder();
		s.append(clazzSender.getSimpleName()).append("=>{");
		Set<String> namespaceReceivers = this.receiverActorPools.keySet();		
		s.append(StringUtil.join(",",namespaceReceivers.toArray())).append("}");
		return s.toString();
	}
	
}