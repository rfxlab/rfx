package rfx.core.stream.functor.common;

import static rfx.core.stream.processor.HttpEventProcessor.COOKIE;
import static rfx.core.stream.processor.HttpEventProcessor.IP;
import static rfx.core.stream.processor.HttpEventProcessor.LOGGEDTIME;
import static rfx.core.stream.processor.HttpEventProcessor.PARTITION_ID;
import static rfx.core.stream.processor.HttpEventProcessor.QUERY;
import static rfx.core.stream.processor.HttpEventProcessor.TOPIC;
import static rfx.core.stream.processor.HttpEventProcessor.USERAGENT;
import rfx.core.stream.functor.BaseFunctor;
import rfx.core.stream.message.Fields;
import rfx.core.stream.message.Tuple;
import rfx.core.stream.model.DataFlowInfo;
import rfx.core.stream.processor.HttpEventProcessor;
import rfx.core.stream.topology.BaseTopology;
import rfx.core.util.LogUtil;
import rfx.core.util.StringUtil;

public class HttpEventTokenizing extends BaseFunctor {	
		
	//what data fields that this actor would send to next actor
	public static final Fields outputFields = new Fields(QUERY, COOKIE,LOGGEDTIME, IP, USERAGENT, TOPIC, PARTITION_ID);
	private HttpEventProcessor processor;
	
	public HttpEventTokenizing(DataFlowInfo dataFlowInfo, BaseTopology topology) {
		super(dataFlowInfo, topology);
		processor = new HttpEventProcessor();
	}

	public void onReceive(Object message) throws Exception {
		if (message instanceof Tuple) {
			this.doPreProcessing();
			Tuple inputTuple = (Tuple) message;
			Tuple outTuple = processor.process(inputTuple, outputFields);			
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