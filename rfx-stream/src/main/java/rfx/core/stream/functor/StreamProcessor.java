package rfx.core.stream.functor;

import rfx.core.stream.message.Tuple;
import rfx.core.stream.model.DataFlowInfo;
import rfx.core.stream.topology.BaseTopology;

/**
 * Base class for Data Pipeline Processing
 * 
 * @author trieunt
 *
 */
public abstract class StreamProcessor extends BaseFunctor {
	
	protected StreamProcessor(DataFlowInfo dataFlowInfo, BaseTopology topology) {
		super(dataFlowInfo, topology);
	}

	@Override
	final public void onReceive(Object message) throws Exception {
		if (message instanceof Tuple) {			
			onReceive((Tuple)message);
		} else {
			unhandled(message);
		}
	}
	
	public abstract void onReceive(Tuple tuple) throws Exception;
}
