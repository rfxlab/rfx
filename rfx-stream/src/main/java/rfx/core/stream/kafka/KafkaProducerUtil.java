package rfx.core.stream.kafka;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

public class KafkaProducerUtil {

	
	static Map<String, Producer<String, String>> kafkaProducerPool = new HashMap<String, Producer<String,String>>();

	public static void addKafkaProducer(String actorId,  Producer<String, String> producer){
		kafkaProducerPool.put(actorId, producer);
	}
	
	public static void closeAndRemoveKafkaProducer(String actorId){
		Producer<String, String> producer = kafkaProducerPool.remove(actorId);
		if(producer != null){
			producer.close();
		}
	}
	
	public static Producer<String, String> getKafkaProducer(String actorId, ProducerConfig producerConfig, boolean refreshProducer){
		Producer<String, String> producer = kafkaProducerPool.get(actorId);
		if(producer == null){
			producer = new Producer<>(producerConfig);
			addKafkaProducer(actorId, producer);
		} else {
			if(refreshProducer){
				producer.close(); producer = null;
				producer = new Producer<>(producerConfig);
				addKafkaProducer(actorId, producer);
			}
		}
		return producer;
	}
	
	public static void closeAllProducers(){
		Collection<Producer<String, String>> producers = kafkaProducerPool.values();
		for (Producer<String, String> producer : producers) {
			producer.close(); producer = null;
		}
	}
	
	
	public static Properties createProducerProperties(String brokerList , String partioner, int batchNumSize){
		//metadata.broker.list accepts input in the form "host1:port1,host2:port2"
		Properties props = new Properties();
		props.put("broker.list", brokerList);				
		props.put("metadata.broker.list", brokerList);
		//async mode Kafka Producer
		//if(httpServerConfigs.getKafkaProducerAsyncEnabled()==1)
		{
			props.put("producer.type", "async");
		}
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("partitioner.class", partioner);
//		props.put("request.required.acks", ""+httpServerConfigs.getKafkaProducerAckEnabled());
//		props.put("message.send.max.retries", ""+httpServerConfigs.getSendKafkaMaxRetries()); // default=3
		props.put("batch.num.messages", ""+batchNumSize);
		props.put("send.buffer.bytes", "1048576");
		props.put("request.timeout.ms", "8000");
		//props.put("queue.enqueue.timeout.ms", "-1");
		//props.put("compression.codec", "1");
		return props;
	}
	
	public static Properties createSynchedAckedProducerProperties(String brokerList , String partioner, int batchNumSize){
		//metadata.broker.list accepts input in the form "host1:port1,host2:port2"
		Properties props = new Properties();
		props.put("broker.list", brokerList);				
		props.put("metadata.broker.list", brokerList);		
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("partitioner.class", partioner);
		props.put("request.required.acks", "1");		
		props.put("batch.num.messages", ""+batchNumSize);
		props.put("send.buffer.bytes", "1048576");
		props.put("request.timeout.ms", "8000");
		return props;
	}
}
