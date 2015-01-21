package rfx.core.stream.functor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import rfx.core.model.CallbackResult;
import rfx.core.stream.listener.Metricable;
import rfx.core.stream.message.Fields;
import rfx.core.stream.message.Tuple;
import rfx.core.stream.message.Values;
import rfx.core.stream.model.DataFlowInfo;
import rfx.core.stream.topology.BaseTopology;
import rfx.core.stream.util.ActorUtil;
import rfx.core.util.StringUtil;
import akka.actor.ActorRef;
import akka.actor.UntypedActor;

/**
 * base class: lambda functional actor, the reactive data processing entity <br>
 * @author trieu
 */
public abstract class BaseFunctor extends UntypedActor implements Metricable{
	
	protected final DataFlowInfo dataFlowInfo;
	protected final BaseTopology topology;
	static Map<String, AtomicLong> counters = new ConcurrentHashMap<String, AtomicLong>();
	String metricKey;
	
	static {
//		Timer timer = new Timer(true);
//		timer.schedule(new TimerTask() {			
//			@Override
//			public void run() {
//				//ClusterDataManager.logMetrics(counters);
//				//TODO
//			}
//		}, 10000, 10000);		
	}
		
	protected BaseFunctor(DataFlowInfo dataFlowInfo, BaseTopology topology) {
		super();
		this.dataFlowInfo = dataFlowInfo;
		this.topology = topology;
		metricKey = StringUtil.toString(getClass().getSimpleName(),"#");
	}	

	/**
	 * emit a tuple from sender to defined receiver in topology 
	 * 
	 * @param newTuple
	 * @param sender
	 */
	protected final void emit(Tuple newTuple, ActorRef sender) {		
		ActorUtil.sendToActor(dataFlowInfo, newTuple, sender);
	}
	
	/**
	 * emit a tuple from sender to defined receiver in topology 
	 * 
	 * @param Tuple newT
	 */
	protected final void emit(Tuple tuple) {		
		ActorUtil.sendToActor(dataFlowInfo, tuple, self());
	}
	
	/**
	 * @param Fields fields
	 * @param String row
	 */
	protected final void emit(Fields fields, String row){
		if(StringUtil.isEmpty(row)){
			return;
		}
		this.emit(new Tuple(fields, new Values(row)), self());		
		doPostProcessing();
	}
	
	/**
	 * emit a tuple from sender to specific receiver 
	 * 
	 * @param newTuple
	 * @param sender
	 * @param namespaceReceiver
	 */
	protected final void emit(Tuple newTuple, ActorRef sender, String namespaceReceiver) {		
		ActorUtil.sendToActor(dataFlowInfo, newTuple, sender, namespaceReceiver);
	}
	
	protected final synchronized CallbackResult<String> doPreProcessing(){
		if(dataFlowInfo.getPreProcessingFunction() != null){
			return dataFlowInfo.getPreProcessingFunction().call();
		}
		return null;
	}
	
	protected final synchronized CallbackResult<String> doPostProcessing(){
		if(dataFlowInfo.getPostProcessingFunction()!= null){
			return dataFlowInfo.getPostProcessingFunction().call();
		}
		return null;
	}
	
	protected final synchronized AtomicLong counter(String metricName) {
		AtomicLong counter = counters.get(metricName);
		if(counter == null){
			counter = new AtomicLong(0);
			counters.put(metricName, counter);
		}
		return counter;
	}
	
	@Override
	public String getMetricKey() {		
		return metricKey;
	}
}
