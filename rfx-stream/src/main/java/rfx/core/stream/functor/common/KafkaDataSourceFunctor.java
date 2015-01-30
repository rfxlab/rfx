package rfx.core.stream.functor.common;

import java.util.List;

import rfx.core.model.CallbackResult;
import rfx.core.stream.kafka.KafkaCallbackResult;
import rfx.core.stream.kafka.KafkaData;
import rfx.core.stream.message.Fields;
import rfx.core.stream.message.KafkaDataPayload;
import rfx.core.stream.message.Tuple;
import rfx.core.stream.message.Values;
import rfx.core.stream.model.DataFlowInfo;
import rfx.core.stream.topology.BaseTopology;
import rfx.core.util.Utils;
import akka.event.Logging;
import akka.event.LoggingAdapter;

public class KafkaDataSourceFunctor extends DataSourceFunctor {
	
	LoggingAdapter log = Logging.getLogger(getContext().system(), this);
	
	public KafkaDataSourceFunctor(DataFlowInfo dataFlowInfo, BaseTopology topology) {
		super(dataFlowInfo, topology);
	}

	//what data fields that this actor would send to next actor
	static Fields fields = new Fields(EVENT,"topic","partitionId","offsetId");
	List<KafkaData> kafkaDatas;
	
	@Override
	public void onReceive(Object message) throws Exception {
		//System.out.println(message + " at " + System.currentTimeMillis());
		if (EMIT_LOG_EVENT.equals(message)) {
			if(isEmitting() || isStopEmitting()){
				System.out.println("--------DataFileSourceFunctor.skip-------"+message);
				unhandled(message);
				return;
			}			
			setEmitting(true);
			
			CallbackResult<String> result = super.doPreProcessing();
			if(result != null){
				if(result instanceof KafkaCallbackResult){					
					KafkaDataPayload data = ((KafkaCallbackResult)result).getKafkaDataPayload();
					if(data != null){
						kafkaDatas = data.getKafkaDataList();
						String topic = data.getTopic();
						int partitionId = data.getPartitionId();
						
						int c = 0;
						for (KafkaData kkData : kafkaDatas) {
							Values values =  new Values(kkData.data(), topic, partitionId, kkData.offset());						
							Tuple newTuple = new Tuple(fields, values);	
							this.emit(newTuple, self());
							if(c % maxSizeToSleep == 0){
								Utils.sleep(timeToSleep);
							}
							super.doPostProcessing();
						}
						kafkaDatas.clear();						
					}					
				}
			}
			
			setEmitting(false);
	    } else if(STOP_EMIT_LOG_EVENT.equals(message)){
	    	stopEmitting();
	    }		
		else {
	      unhandled(message);
	    }
	}
}