package server.kafka;
import static io.netty.handler.codec.http.HttpHeaders.Names.COOKIE;
import static io.netty.handler.codec.http.HttpHeaders.Names.REFERER;
import static io.netty.handler.codec.http.HttpHeaders.Names.USER_AGENT;

import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;

import kafka.producer.ProducerConfig;

import org.vertx.java.core.MultiMap;
import org.vertx.java.core.http.HttpServerRequest;

import rfx.core.util.HttpRequestUtil;
import rfx.core.util.LogUtil;
import rfx.core.util.StringPool;
import rfx.core.util.StringUtil;
import rfx.core.util.Utils;
import server.http.configs.KafkaProducerConfigs;

public class HttpLogKafkaHandler implements KafkaLogHandler {
	
	static KafkaProducerConfigs httpServerConfigs = KafkaProducerConfigs.load();	
	
	//TODO optimize this config
	public final static int MAX_KAFKA_TO_SEND = 900;
	public final static long TIME_TO_SEND = 5000; //in milisecond
	public static int NUM_BATCH_JOB = httpServerConfigs.getNumberBatchJob();
	public static int SEND_KAFKA_THREAD_PER_BATCH_JOB = httpServerConfigs.getSendKafkaThreadPerBatchJob();
		
	private static Map<String, HttpLogKafkaHandler> _kafkaHandlerList = new HashMap<>();
	
	static boolean writeToKafka =  httpServerConfigs.getWriteKafkaLogEnable() == 1;
	AtomicLong counter = new AtomicLong();

	private List<SendLogBufferTask> logBufferList = new ArrayList<>(NUM_BATCH_JOB);
	private Timer timer = new Timer();	
	private Random randomGenerator;
			
	private ProducerConfig producerConfig;

	private String topic = "";
	private int maxSizeBufferToSend = MAX_KAFKA_TO_SEND;
	
	public void countingToDebug() {
		long c = counter.addAndGet(1);
		System.out.println(this.topic + " logCounter: " + c);		
	}
	
	public static void initKafkaSession() {			
		try {
			Map<String,Map<String,String>> kafkaProducerList = httpServerConfigs.getKafkaProducerList();
			Set<String> keys = kafkaProducerList.keySet();
			System.out.println(keys);
			String defaultPartitioner = httpServerConfigs.getDefaultPartitioner();
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
				ProducerConfig producerConfig = new ProducerConfig(configs);
				HttpLogKafkaHandler kafkaInstance = new HttpLogKafkaHandler(producerConfig, topic);
				_kafkaHandlerList.put(key, kafkaInstance);
				LogUtil.i("KafkaHandler.init-loaded: "+ key + " => "+jsonProducerConfig);
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}		
	}
	
	
	
	public void shutdown() {
		//executor.shutdown();
		//producer.close();		
		timer.cancel();
	}
	
	/*
	 * Get Singleton by specified config
	 */
	public static HttpLogKafkaHandler getKafkaHandler(String kafkaType){		
		return _kafkaHandlerList.get(kafkaType);
	}
	
	protected HttpLogKafkaHandler(ProducerConfig producerConfig, String topic) {
		this.producerConfig 	= 	producerConfig;
		this.topic 				= 	topic;
		initTheTimer();
	}	
		
	public int getMaxSizeBufferToSend() {
		return maxSizeBufferToSend;
	}

	public void setMaxSizeBufferToSend(int maxSizeBufferToSend) {
		this.maxSizeBufferToSend = maxSizeBufferToSend;
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
					Utils.sleep(1000);
				}	
			}
		}, 15000, 15000);
		randomGenerator = new Random();
	}	
	
	@Override
	public void writeLogToKafka(String ip, String userAgent, String logDetails, String cookieString){
		if( ! writeToKafka){
			//skip write logs to Kafka
			return;
		}		
		countingToDebug();			
		int index = randomGenerator.nextInt(logBufferList.size());
		try {
			SendLogBufferTask task = logBufferList.get(index);
			if(task != null){
				HttpDataLog log = new HttpDataLog(ip, userAgent, logDetails, cookieString);
				System.out.println("log " + log);
				task.addToBufferQueue(log);
			} else {
				LogUtil.e(topic, "writeLogToKafka: FlushLogToKafkaTask IS NULL");
			}
		} catch (Exception e) {
			LogUtil.e(topic, "writeLogToKafka: "+e.getMessage());
		}
	}
	
	@Override
	public void flushAllLogsToKafka() {
		for (SendLogBufferTask task : logBufferList) {
			//flush all
			task.flushLogsToKafkaBroker(-1);
		}		
	}

	@Override
	public void writeLogToKafka(HttpServerRequest request){	
		String uri = request.uri();
    	String remoteIp = HttpRequestUtil.getRemoteIP(request);
		if(StringUtil.isEmpty(uri)){
			return;
		}
		int idx = uri.indexOf("?");
		if(idx < 0){
			return;
		}
		String logDetails = uri.substring(idx+1);
		if(StringUtil.isEmpty(logDetails)){
			return;
		}
		MultiMap headers = request.headers();
		String referer = headers.get(REFERER);
		String userAgent = headers.get(USER_AGENT);
		
		//cookie check & build
		String cookie = headers.get(COOKIE);
		StringBuilder cookieStBuilder = new StringBuilder();
		if( StringUtil.isNotEmpty(cookie) ){
			try {
				cookie = URLDecoder.decode(cookie,StringPool.UTF_8);
			} catch (Exception e1) {}
			cookieStBuilder.append(cookie);
		}				
		if(StringUtil.isNotEmpty(referer)){
			cookieStBuilder.append("; referer=").append(referer);
		}		
		writeLogToKafka(remoteIp, userAgent, logDetails, cookieStBuilder.toString());		
	}
	

}
