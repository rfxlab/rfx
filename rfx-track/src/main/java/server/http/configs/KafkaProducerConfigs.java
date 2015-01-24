package server.http.configs;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import rfx.core.util.FileUtils;
import rfx.core.util.LogUtil;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

/**
 * Kafka Producer Config
 * 
 * @author trieu
 *
 */
public class KafkaProducerConfigs implements Serializable {	
	static KafkaProducerConfigs _instance;
	private static final long serialVersionUID = 4936959262031389418L;
		
	int writeKafkaLogEnable = 1;
	int numberBatchJob = 20;
	int timeSendKafkaPerBatchJob = 4000;
	int sendKafkaThreadPerBatchJob = 3;
	int sendKafkaMaxRetries = 100; 
	int kafkaProducerAsyncEnabled = 1;
	int kafkaProducerAckEnabled = 1;
	
	String defaultPartitioner = "";
	Map<String,Map<String,String>> kafkaProducerList;	

	public KafkaProducerConfigs() {
		super();
	}
	
	public int getKafkaProducerAsyncEnabled() {
		return kafkaProducerAsyncEnabled;
	}

	public void setKafkaProducerAsyncEnabled(int kafkaProducerAsyncEnabled) {
		this.kafkaProducerAsyncEnabled = kafkaProducerAsyncEnabled;
	}

	public int getKafkaProducerAckEnabled() {
		return kafkaProducerAckEnabled;
	}

	public void setKafkaProducerAckEnabled(int kafkaProducerAckEnabled) {
		this.kafkaProducerAckEnabled = kafkaProducerAckEnabled;
	}

	public int getSendKafkaMaxRetries() {
		return sendKafkaMaxRetries;
	}

	public void setSendKafkaMaxRetries(int sendKafkaMaxRetries) {
		this.sendKafkaMaxRetries = sendKafkaMaxRetries;
	}

	public int getNumberBatchJob() {
		return numberBatchJob;
	}

	public void setNumberBatchJob(int numberBatchJob) {
		this.numberBatchJob = numberBatchJob;
	}
	
	public int getWriteKafkaLogEnable() {
		return writeKafkaLogEnable;
	}

	public void setWriteKafkaLogEnable(int writeKafkaLogEnable) {
		this.writeKafkaLogEnable = writeKafkaLogEnable;
	}
	
	public Map<String, Map<String, String>> getKafkaProducerList() {
		if(kafkaProducerList == null){
			kafkaProducerList = new HashMap<>();
		}
		return kafkaProducerList;
	}

	public void setKafkaProducerList(
			Map<String, Map<String, String>> kafkaProducerList) {
		this.kafkaProducerList = kafkaProducerList;
	}

	@Override
	public String toString() {
		StringBuilder s = new StringBuilder();

		return s.toString();
	}
	
	
	public static final KafkaProducerConfigs load(String configPath) {
		if (_instance == null) {
			try {
				String json = FileUtils.readFileAsString(configPath);				
				_instance = new Gson().fromJson(json, KafkaProducerConfigs.class);
				LogUtil.i("HttpServerConfigs loaded and create new instance from "+ HttpLogServerCommon.KAFKA_PRODUCER_CONFIG_FILE);
			} catch (Exception e) {
				if (e instanceof JsonSyntaxException) {
					e.printStackTrace();
					System.err.println("Wrong JSON syntax in file "+HttpLogServerCommon.KAFKA_PRODUCER_CONFIG_FILE);
				} else {
					e.printStackTrace();
				}
			}
		}
		return _instance;
	}
	
	public static final KafkaProducerConfigs load() {
		return load(HttpLogServerCommon.KAFKA_PRODUCER_CONFIG_FILE);
	}
	
	public int getSendKafkaThreadPerBatchJob() {
		return sendKafkaThreadPerBatchJob;
	}

	public void setSendKafkaThreadPerBatchJob(int sendKafkaThreadPerBatchJob) {
		this.sendKafkaThreadPerBatchJob = sendKafkaThreadPerBatchJob;
	}

	public int getTimeSendKafkaPerBatchJob() {
		return timeSendKafkaPerBatchJob;
	}

	public void setTimeSendKafkaPerBatchJob(int timeSendKafkaPerBatchJob) {
		this.timeSendKafkaPerBatchJob = timeSendKafkaPerBatchJob;
	}

	public String getDefaultPartitioner() {
		return defaultPartitioner;
	}

	public void setDefaultPartitioner(String defaultPartitioner) {
		this.defaultPartitioner = defaultPartitioner;
	}	
}