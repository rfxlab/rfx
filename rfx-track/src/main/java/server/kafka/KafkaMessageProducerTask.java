package server.kafka;

import java.util.List;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import rfx.core.util.LogUtil;

public class KafkaMessageProducerTask implements Runnable  {
	
	List<ProducerRecord<String, String>> batchLogs;
	private String actorId;
	Producer<String, String> producer;

	public KafkaMessageProducerTask(String actorId, Producer<String, String> producer, List<ProducerRecord<String, String>> batchLogs) {		
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
				System.out.println("FlushHttpDataLogsTask "+this.actorId+" batchsize = "+batchLogs.size());
				for (ProducerRecord<String, String> producerRecord : batchLogs) {
					producer.send(producerRecord);
				}
				
			} catch (Exception e) {				
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