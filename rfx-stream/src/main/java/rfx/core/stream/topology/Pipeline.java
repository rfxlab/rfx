package rfx.core.stream.topology;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import rfx.core.stream.model.DataFlowInfo;
import akka.actor.ActorRef;

/**
 * 
 * simplify building pipeline of topology
 * 
 * @author trieu
 *
 */
public class Pipeline {
	DataFlowInfo senderinfo;
	int defaultPoolSize = 5000;
	BaseTopology topology;
	private Queue<Class<?>> functorQueue = new LinkedList<Class<?>>();
	
	public Pipeline(int defaultPoolSize, BaseTopology topology) {
		super();
		this.defaultPoolSize = defaultPoolSize;
		this.topology = topology;
	}
	
	public Pipeline(BaseTopology topology) {
		super();		
		this.topology = topology;
	}

	public static Pipeline create(int defaultPoolSize, BaseTopology topology){
		return new Pipeline(defaultPoolSize, topology);
	}
	
	public static Pipeline create(BaseTopology topology){
		return new Pipeline(topology);
	}
	
	void addFunctorToTopology(Class<?> functorClass, int poolSize){
		DataFlowInfo dfInfo = new DataFlowInfo(functorClass);
		Map<String, ActorRef> actorsPool = topology.createActorPool(functorClass,dfInfo, poolSize);
		
		if(senderinfo == null){
			//the first will be received logs from Kafka Data Seeders
			topology.setReceiverFromEmitter(functorClass, actorsPool);
		} else {
			senderinfo.addReceiverActorPool(functorClass, actorsPool);
		}
		senderinfo = dfInfo;	
	}
		
	public Pipeline apply(Class<?> functorClass){
		functorQueue.add(functorClass);
		return this;
	}
	
	public Pipeline apply(Class<?> functorClass, int poolSize){
		addFunctorToTopology(functorClass, poolSize);	
		return this;
	}
	
	public BaseTopology done(){
		int phases = functorQueue.size();
		if(phases > 0){
			senderinfo = null;	
			this.defaultPoolSize = computeDefaultMaxPoolSize(phases);
			while ( true ) {
				Class<?> functorClass = functorQueue.poll();
				if(functorClass != null){
					addFunctorToTopology(functorClass, defaultPoolSize);
				} else {
					break;
				}
			}
		}		
		senderinfo = null;
		return topology;
	}
	
	public static int computeDefaultMaxPoolSize(int phases){
		long cores = Runtime.getRuntime().availableProcessors()*1L;
		long mem = Runtime.getRuntime().maxMemory();		
		int maxPoolSize = (int) Math.floor((mem / cores) / (6000 * phases  ));
		//System.out.println(maxPoolSize);
		return maxPoolSize;
	}
}
