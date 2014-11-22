package rfx.sample.analytics;

import java.util.concurrent.atomic.AtomicLong;

import rfx.core.stream.functor.BaseFunctor;
import rfx.core.stream.message.Tuple;
import rfx.core.stream.model.DataFlowInfo;
import rfx.core.stream.topology.BaseTopology;

public class RawLogTokenizer extends BaseFunctor{
	
	protected RawLogTokenizer(DataFlowInfo dataFlowInfo, BaseTopology topology) {
		super(dataFlowInfo, topology);
	}
	
	static AtomicLong genId = new AtomicLong();
	
	public static final long getCurrentRowCount(){
		return genId.get();
	}
	
	@Override
	public void onReceive(Object message) throws Exception {
		if (message instanceof Tuple) {			
			Tuple inputTuple = (Tuple) message;
			try {				
				String row = inputTuple.getStringByField("row");
				
				System.out.println(genId.incrementAndGet() + " # ");
				String[] toks = row.split("\t");
				//System.out.println(toks.length);
				
			} catch (Exception e) {
				e.printStackTrace();				
				//log.error(ExceptionUtils.getStackTrace(e));
			} finally {
				inputTuple.clear();
			}		
			this.doPostProcessing();
		} else {
			unhandled(message);
		}
	}
}
