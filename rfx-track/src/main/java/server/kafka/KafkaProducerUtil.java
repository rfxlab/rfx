package server.kafka;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import server.http.configs.KafkaProducerConfigs;

public class KafkaProducerUtil {

	static KafkaProducerConfigs kafkaProducerConfigs = KafkaProducerConfigs.load();	
	static Map<String, Producer<String, String>> kafkaProducerPool = new ConcurrentHashMap<>();

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
			producer = new KafkaProducer<>(producerConfig.originals());
			addKafkaProducer(actorId, producer);
		} else {
			if(refreshProducer){
				System.out.println("### refreshProducer for actorId: " + actorId);
				producer.close(); producer = null;
				producer = new KafkaProducer<>(producerConfig.originals());
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
	
	
	public static Properties createProducerProperties(String clientId, String brokerList , String partioner, int batchNumSize){
		//metadata.broker.list accepts input in the form "host1:port1,host2:port2"

		Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, partioner);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchNumSize);
        props.put(ProducerConfig.ACKS_CONFIG, kafkaProducerConfigs.getKafkaProducerAsyncEnabled()==1);
        
		return props;
	}
	
}
