package server.kafka;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import kafka.producer.ProducerConfig;
import rfx.core.util.LogUtil;
import rfx.core.util.Utils;
import server.http.configs.KafkaProducerConfigs;

/**
 * Kafka Event Data Producer Handler
 * 
 * @author trieunt
 *
 */
public class KafkaProducerHandler {
	static KafkaProducerConfigs kafkaProducerConfigs = KafkaProducerConfigs.load();
	public static final boolean KAFKA_ENABLED =  kafkaProducerConfigs.getWriteKafkaLogEnable() == 1;
	
	public final static int MAX_KAFKA_TO_SEND = 900;
	public final static long TIME_TO_SEND = 5000; //in milisecond
	public final static int NUM_BATCH_JOB = kafkaProducerConfigs.getNumberBatchJob();
	public final static int SEND_KAFKA_THREAD_PER_BATCH_JOB = kafkaProducerConfigs.getSendKafkaThreadPerBatchJob();
	final static String defaultPartitioner = kafkaProducerConfigs.getDefaultPartitioner();
		
	private static Map<String, KafkaProducerHandler> kafkaProducers = new HashMap<>();
		
	private Random randomGenerator = new Random();
	private Timer timer = new Timer(true);
	
	private List<SendLogBufferTask> logBufferList = new ArrayList<>(NUM_BATCH_JOB);
	private int maxSizeBufferToSend = MAX_KAFKA_TO_SEND;
	private ProducerConfig producerConfig;
	private String topic;
	
	private KafkaProducerHandler(ProducerConfig producerConfig, String topic) {
		this.producerConfig 	= 	producerConfig;
		this.topic 				= 	topic;
		initTheTimer();
	}
	
	static {
		init();
	}
	
	public static void init() {		
		if(kafkaProducers.size() > 0){
			//already init
			return;
		}
		try {
			Map<String,Map<String,String>> kafkaProducerList = kafkaProducerConfigs.getKafkaProducerList();
			Set<String> keys = kafkaProducerList.keySet();
			System.out.println(keys);
			
			System.out.println("### kafkaProducerList " + kafkaProducerList.size());
			for (String key : keys) {
				Map<String,String> jsonProducerConfig = kafkaProducerList.get(key);
				String topic = jsonProducerConfig.get("kafkaTopic");
				String brokerList =  jsonProducerConfig.get("brokerList");			
				String partioner =  jsonProducerConfig.get("partioner");
				if (partioner == null ) {
					partioner = defaultPartitioner;
				}							
				Properties configs = KafkaProducerUtil.createProducerProperties(brokerList, partioner,MAX_KAFKA_TO_SEND);
				kafkaProducers.put(key, new KafkaProducerHandler(new ProducerConfig(configs), topic));
				LogUtil.i("KafkaHandler.init-loaded: "+ key + " => "+jsonProducerConfig);
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}		
	}
	
	void initTheTimer(){
		int delta = 400;
		for (int i = 0; i < NUM_BATCH_JOB; i++) {
			int id = i+1;
			SendLogBufferTask job = new SendLogBufferTask(producerConfig, topic,id);
			timer.schedule(job,delta , TIME_TO_SEND );
			delta += 100;
			logBufferList.add(job);
		}
		timer.schedule(new TimerTask() {			
			@Override
			public void run() {
				for (SendLogBufferTask task : logBufferList) {					
					task.setRefreshProducer(true);
					Utils.sleep(10000);
				}	
			}
		}, 15000, 15000);		
	}
	
	public int getMaxSizeBufferToSend() {
		return maxSizeBufferToSend;
	}

	public void setMaxSizeBufferToSend(int maxSizeBufferToSend) {
		this.maxSizeBufferToSend = maxSizeBufferToSend;
	}
	
	public void shutdown() {
		timer.cancel();
	}
	
	public String getTopic() {
		return topic;
	}
	
	/*
	 * Get Singleton by specified config
	 */
	public static KafkaProducerHandler getKafkaHandler(String producerKey){		
		return kafkaProducers.get(producerKey);
	}
	
	/**
	 * write data to kafka
	 * 
	 * @param EventData data
	 */
	public void writeData(EventData data){
		int index = randomGenerator.nextInt(logBufferList.size());
		try {
			SendLogBufferTask task = logBufferList.get(index);
			if(task != null){				
				//System.out.println("log " + log);
				task.addToBufferQueue(data);
			} else {
				LogUtil.e(topic, "writeLogToKafka: FlushLogToKafkaTask IS NULL");
			}
		} catch (Exception e) {
			LogUtil.e(topic, "writeLogToKafka: "+e.getMessage());
		}
	}
	
	
	/**
	 * flush all
	 */
	public void flushAllLogsToKafka() {
		for (SendLogBufferTask task : logBufferList) {			
			task.flushLogsToKafkaBroker(-1);
		}		
	}
}
