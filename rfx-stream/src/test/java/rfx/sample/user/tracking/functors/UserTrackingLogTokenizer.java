package rfx.sample.user.tracking.functors;

import rfx.core.stream.functor.BaseFunctor;
import rfx.core.stream.functor.common.DataSourceFunctor;
import rfx.core.stream.message.Fields;
import rfx.core.stream.message.Tuple;
import rfx.core.stream.message.Values;
import rfx.core.stream.model.DataFlowInfo;
import rfx.core.stream.processor.Processor;
import rfx.core.stream.topology.BaseTopology;
import rfx.core.util.LogUtil;
import rfx.core.util.StringUtil;

public class UserTrackingLogTokenizer extends BaseFunctor {

	// what data fields that this actor would send to next actor
	static Fields outputFields = new Fields("loggedtime", "uuid", "event","url", "topic", "partitionId");
	UserTrackingLogProcessor processor;

	public UserTrackingLogTokenizer(DataFlowInfo dataFlowInfo,
			BaseTopology topology) {
		super(dataFlowInfo, topology);
		processor = new UserTrackingLogProcessor();
	}

	public void onReceive(Object message) throws Exception {
		if (message instanceof Tuple) {
			this.doPreProcessing();
			Tuple inputTuple = (Tuple) message;
			Tuple outTuple = processor.process(inputTuple, outputFields);

			if (outTuple != null) {
				// output to next phase
				this.emit(outTuple, self());
				
				String monitorkey = StringUtil.toString(this.getMetricKey(),outTuple.getStringByField("topic"), "#", outTuple.getIntegerByField("partitionId"));
				this.counter(monitorkey).incrementAndGet();
				this.topology.counter().incrementAndGet();
			} else {
				LogUtil.error("logTokens.length (delimiter is tab) is NOT = 5, INVALID LOG ROW FORMAT ");
			}
			inputTuple.clear();
		} else {
			unhandled(message);
		}
	}
	
	static class UserTrackingLogProcessor extends Processor{
		@Override
		public Tuple process(Tuple inputTuple, Fields outputMetadataFields) {
			String logRow = inputTuple.getStringByField(DataSourceFunctor.EVENT);
	    	String topic = inputTuple.getStringByField("topic");
	    	String partitionId = inputTuple.getStringByField("partitionId");	    	
			String[] logTokens = logRow.split("\t");		  	

			if(logTokens.length == 4){
				if(	   StringUtil.isNotEmpty(logTokens[0])
					&& StringUtil.isNotEmpty(logTokens[1]) 
					&& StringUtil.isNotEmpty(logTokens[2]) 
					&& StringUtil.isNotEmpty(logTokens[3])
					){
					
					long loggedtime = StringUtil.safeParseLong(logTokens[0]);
					String uuid = StringUtil.safeString(logTokens[1]);
					String event = logTokens[2];
					String url = StringUtil.safeString(logTokens[3]);	
				
					return new Tuple(outputMetadataFields, new Values(loggedtime ,uuid ,event, url ,topic ,partitionId));
				}
			}	
			return null;
		}		
	}
}