package server.kafka;

import org.vertx.java.core.http.HttpServerRequest;

import server.http.model.HttpEventKafkaLog;

public interface KafkaLogHandler {
	
	/**
	 * Asynchronous push log data queue, the timer will schedule a job for sending to Kafka to avoid locking response
	 * 
	 * @param ip
	 * @param request
	 */
	public abstract void writeLogToKafka(HttpEventKafkaLog el);

	public abstract void writeLogToKafka(HttpServerRequest req);
	
	public abstract void flushAllLogsToKafka(); 	
}