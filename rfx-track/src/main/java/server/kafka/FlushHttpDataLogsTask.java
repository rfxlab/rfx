package server.kafka;

import java.util.List;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import rfx.core.util.LogUtil;

public class FlushHttpDataLogsTask implements Runnable  {
	
	List<KeyedMessage<String, String>> batchLogs;
	private String actorId;
	Producer<String, String> producer;

	public FlushHttpDataLogsTask(String actorId, Producer<String, String> producer, List<KeyedMessage<String, String>> batchLogs) {		
		this.actorId = actorId;
		this.producer = producer;
		this.batchLogs = batchLogs;
		if(this.batchLogs == null){
			throw new IllegalArgumentException("batchLogs CAN NOT BE NULL");
		}
	}
	
	@Override
	public void run() {			
		if(producer != null && batchLogs.size()>0){
			try {				
				System.out.println("FlushHttpDataLogsTask "+this.actorId+" batchsize = "+batchLogs.size() + " "+producer);
				producer.send(batchLogs);
			} catch (Exception e) {
				e.printStackTrace();
				LogUtil.e("FlushHttpDataLogsTask", "sendToKafka fail : "+e.getMessage());	
				//close & open the Kafka Connection manually				
				KafkaProducerUtil.closeAndRemoveKafkaProducer(actorId);
			} finally {
				batchLogs.clear();
				batchLogs = null;
			}	
		} else {
			LogUtil.e("FlushHttpDataLogsTask", "producer is NULL for actorId:" + actorId);
		}
		
	}
}