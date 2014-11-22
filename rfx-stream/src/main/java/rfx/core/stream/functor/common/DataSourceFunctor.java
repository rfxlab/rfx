package rfx.core.stream.functor.common;

import java.util.concurrent.atomic.AtomicInteger;

import rfx.core.stream.functor.BaseFunctor;
import rfx.core.stream.model.DataFlowInfo;
import rfx.core.stream.topology.BaseTopology;

/**
 * the base class for data source functor, that listen and emitting data to all processing functor
 * 
 * @author trieu <br>
 *
 */
public abstract class DataSourceFunctor extends BaseFunctor {

	public static final String EMIT_LOG_EVENT = "EmitLogEvent";
	public static final String STOP_EMIT_LOG_EVENT = "StopEmitLogEvent";
	private boolean isEmitting = false;
	private boolean stopEmitting = false;
	protected static int maxSizeToSleep = 5000;
	protected static long timeToSleep = 5;
	protected static int maxConcurrentEmitter = 4;
	protected static AtomicInteger workingEmitterCount = new AtomicInteger(0);

	public DataSourceFunctor(DataFlowInfo dataFlowInfo, BaseTopology topology) {
		super(dataFlowInfo, topology);
	}
	
	

	public static void setMaxSizeToSleep(int maxSizeToSleep) {
		DataSourceFunctor.maxSizeToSleep = maxSizeToSleep;
	}

	public static void setTimeToSleep(long milisecs) {
		DataSourceFunctor.timeToSleep = milisecs;
	}
	
	public static void setMaxConcurrentEmitter(int maxConcurrentEmitter) {
		DataSourceFunctor.maxConcurrentEmitter = maxConcurrentEmitter;
	}

	public boolean isEmitting() {
		return isEmitting;
	}

	public synchronized boolean stopEmitting() {
		stopEmitting = true;		
		return stopEmitting;
	}

	public synchronized boolean restartEmitting() {
		stopEmitting = false;		
		return stopEmitting;
	}

	public boolean isStopEmitting() {
		return stopEmitting;
	}

	public synchronized void setStopEmitting(boolean stopEmitting) {
		this.stopEmitting = stopEmitting;
	}

	public static int getMaxSizeToSleep() {
		return maxSizeToSleep;
	}

	public synchronized void setEmitting(boolean isEmitting) {
		this.isEmitting = isEmitting;
		if(isEmitting){
			workingEmitterCount.incrementAndGet();	
		} else {
			workingEmitterCount.decrementAndGet();
		}		
	}
	
	public boolean isOverMaxConcurrentEmitter() {
		return workingEmitterCount.get() > maxConcurrentEmitter;
	}

	
}