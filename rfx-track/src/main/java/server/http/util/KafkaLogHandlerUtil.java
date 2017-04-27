package server.http.util;

import com.google.gson.Gson;

import io.vertx.core.http.HttpServerRequest;
import rfx.core.util.StringUtil;
import server.http.handler.kafka.HttpEventKafkaHandler;
import server.http.model.HttpEventKafkaLog;

public class KafkaLogHandlerUtil {
	
	public static void log(final HttpServerRequest req, String producerKey){		
		HttpEventKafkaHandler kafkaHandler = HttpEventKafkaHandler.loadHandler(producerKey);
		if (kafkaHandler != null) {
			kafkaHandler.writeLogToKafka(req);
		} else {
			System.err.println("No KafkaLogHandler found for " + producerKey);
		}		
	}
	
	public static void logAndResponseImage1px(final HttpServerRequest req, String producerKey){
		log(req, producerKey);
		HttpTrackingUtil.trackingResponse(req);
	}
	
	public static void logDataToKafka(HttpServerRequest req, String json){
		HttpTrackingUtil.trackingResponse(req);
		logRequestToKafka(json);
	}
	
	public static void logRequestToKafka(String json){		
		if(StringUtil.isEmpty(json)){
			return;
		}
		try {
			HttpEventKafkaLog el = new Gson().fromJson(json, HttpEventKafkaLog.class);
			System.out.println("logRequestToKafka "+json);
			String kafkaType = el.getEventType();
			HttpEventKafkaHandler kafkaHandler = HttpEventKafkaHandler.loadHandler(kafkaType);
			if (kafkaHandler != null) {
				kafkaHandler.writeLogToKafka(el);
			} else {
				System.err.println("No KafkaLogHandler found for " + kafkaType);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}		
	}

}
