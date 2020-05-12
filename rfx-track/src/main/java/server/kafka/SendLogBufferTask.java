package server.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import rfx.core.util.LogUtil;
import rfx.core.util.Utils;
import server.http.configs.KafkaProducerConfigs;

public class SendLogBufferTask extends TimerTask {

	private ProducerConfig producerConfig;
	private String topic = "";
	private String actorId;
	private Queue<EventData> queueLogs;
	private ExecutorService executor;
	int numberFlushOfJob = 3;
	boolean refreshProducer = false;
	
	public String getId() {
		return actorId;
	}

	public void setId(String id) {
		this.actorId = id;
	}
	
	public void addToBufferQueue(EventData data){
		queueLogs.add(data);		
	}
	
	public boolean isRefreshProducer() {
		return refreshProducer;
	}

	public synchronized void setRefreshProducer(boolean refreshProducer) {
		int queueSize = this.queueLogs.size();
		if(queueSize > 0){
			System.out.println("actorId: " + actorId + " got refreshProducer="+refreshProducer);
			this.refreshProducer = refreshProducer;
		}
	}

	public SendLogBufferTask(ProducerConfig producerConfig, String topic, int id) {
		super();
		KafkaProducerConfigs configs = KafkaProducerConfigs.load(); 
		this.producerConfig = producerConfig;
		this.topic = topic;		
		this.actorId = topic+"-"+id;
		this.queueLogs = new ConcurrentLinkedQueue<>();
		executor = Executors.newFixedThreadPool(configs.getSendKafkaThreadPerBatchJob());
	}
	
	public void flushLogsToKafkaBroker(int maxSize){		
		int queueSize = this.queueLogs.size();
		if(maxSize <= 0){
			maxSize = queueSize;			
		} 
		
		if(queueSize > 0){
			try {
				//init the batch list with max capacity
				List<ProducerRecord<String, String>> batchLogs = new ArrayList<>(queueSize);
				int batchSize = 0;
				while( true ){									
					EventData data = this.queueLogs.poll();
					if(data == null){
						break;
					} else {					
						//build the payload, and add to the list
						ProducerRecord<String, String> payload = new ProducerRecord<String, String>(this.topic, data.toStringMessage());	
						batchLogs.add(payload);						
						batchSize = batchLogs.size();
						if(batchSize >= maxSize && maxSize > 0){
							break;
						}
					}
				}
				//send as list, then force close connection
				if(batchSize > 0 && maxSize > 0){				
					System.out.println( "producer.send , batchLogs size : " + batchLogs.size());
					Producer<String, String> producer = KafkaProducerUtil.getKafkaProducer(actorId, producerConfig, refreshProducer);
					executor.execute(new KafkaMessageProducerTask(actorId, producer, batchLogs));
				}
			} catch (Exception e) {			
				e.printStackTrace();
				LogUtil.e(topic, "sendToKafka fail : "+e.getMessage());			
			} 
		}
		
	}
	
	public void flushLogsToKafkaBroker(){			
		flushLogsToKafkaBroker(KafkaProducerHandler.MAX_KAFKA_TO_SEND);
	}
	
	@Override
	public void run() {	
		for (int i = 0; i < numberFlushOfJob; i++) {
			flushLogsToKafkaBroker();
			if(refreshProducer){				
				setRefreshProducer(false);
				Utils.sleep(10);
			}
			Utils.sleep(2);
		}
	}
}
