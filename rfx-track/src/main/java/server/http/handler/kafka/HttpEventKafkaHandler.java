package server.http.handler.kafka;

import java.util.HashMap;
import java.util.Map;

import org.vertx.java.core.http.HttpServerRequest;

import server.http.model.HttpEventKafkaLog;

public abstract class HttpEventKafkaHandler {
	
	/**
	 * Asynchronous push log data queue, the timer will schedule a job for sending to Kafka to avoid locking HTTP response
	 * 
	 * @param ip
	 * @param request
	 */
	public abstract void writeLogToKafka(HttpEventKafkaLog el);

	public abstract void writeLogToKafka(HttpServerRequest req);
	
	static Map<String,HttpEventKafkaHandler> handlers = new HashMap<>();
	public static HttpEventKafkaHandler loadHandler(String producerKey){
		HttpEventKafkaHandler h = handlers.get(producerKey);
		if(h == null){
			h = new HttpEventKafkaHandlerImpl(producerKey);
			handlers.put(producerKey, h);
		}
		return h;
	}
}