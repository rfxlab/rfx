package rfx.core.stream.functor.common;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import rfx.core.stream.functor.BaseFunctor;
import rfx.core.stream.model.DataFlowInfo;
import rfx.core.stream.topology.BaseTopology;

public abstract class AggregateFunctor extends BaseFunctor {

	private Map<Integer, Queue<Object>> timeseriesQueue = new HashMap<Integer, Queue<Object>>(); 
	
	protected AggregateFunctor(DataFlowInfo dataFlowInfo, BaseTopology topology) {
		super(dataFlowInfo, topology);
	}
	
	protected void addEvent(Object event) {
		int secKey = (int)(System.currentTimeMillis() / 1000L);
		Queue<Object> queue = timeseriesQueue.get(secKey);
		if(queue == null){
			queue = new LinkedList<>();
			queue.add(event);
			timeseriesQueue.put(secKey, queue);
		} else {
			queue.add(event);
		}
	}

}
