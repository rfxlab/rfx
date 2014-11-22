package rfx.core.stream.emitters;

import rfx.core.stream.listener.Metricable;
import rfx.core.stream.model.DataFlowPreProcessing;

public abstract class DataEmitter extends DataFlowPreProcessing implements Metricable {
	
	public DataEmitter() {
		super();
	}	
	
	public String getMetricKey(){
		return getClass().getSimpleName()+"#";
	}
}
