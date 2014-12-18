package rfx.core.stream.functor.common;

import static rfx.core.stream.processor.TokenProcessor.COOKIE;
import static rfx.core.stream.processor.TokenProcessor.IP;
import static rfx.core.stream.processor.TokenProcessor.LOGGEDTIME;
import static rfx.core.stream.processor.TokenProcessor.PARTITION_ID;
import static rfx.core.stream.processor.TokenProcessor.QUERY;
import static rfx.core.stream.processor.TokenProcessor.TOPIC;
import static rfx.core.stream.processor.TokenProcessor.USERAGENT;
import rfx.core.stream.functor.BaseFunctor;
import rfx.core.stream.message.Fields;
import rfx.core.stream.message.Tuple;
import rfx.core.stream.model.DataFlowInfo;
import rfx.core.stream.processor.TokenProcessor;
import rfx.core.stream.topology.BaseTopology;
import rfx.core.util.LogUtil;
import rfx.core.util.StringUtil;

public class TokenizingDefaultEventLog extends BaseFunctor {	
		
	//what data fields that this actor would send to next actor
	public static final Fields outputFields = new Fields(QUERY, COOKIE,LOGGEDTIME, IP, USERAGENT, TOPIC, PARTITION_ID);
	private TokenProcessor processor;
	
	public TokenizingDefaultEventLog(DataFlowInfo dataFlowInfo, BaseTopology topology) {
		super(dataFlowInfo, topology);
		processor = new TokenProcessor();
	}

	public void onReceive(Object message) throws Exception {
		if (message instanceof Tuple) {
			this.doPreProcessing();
			Tuple inputTuple = (Tuple) message;
			Tuple outTuple = processor.processToTuple(inputTuple, outputFields);			
			if(outTuple != null){	
				//output to next phase										
				this.emit(outTuple, self());
				this.counter(StringUtil.toString(this.getMetricKey(), outTuple.getStringByField("topic") + "#" +outTuple.getIntegerByField("partitionId"))).incrementAndGet();
				this.topology.counter().incrementAndGet();
			} else {
			    LogUtil.error("logTokens.length (delimiter is tab) is NOT = 5, INVALID LOG ROW FORMAT ");
			} 				
			inputTuple.clear();
		} else {
			unhandled(message);
		}
	}
}