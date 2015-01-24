package server.http.util;

import static io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaders.Names.USER_AGENT;

import org.vertx.java.core.MultiMap;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.json.impl.Base64;

import rfx.core.util.SecurityUtil;
import rfx.core.util.StringUtil;
import server.http.handler.kafka.HttpEventKafkaHandler;
import server.http.model.HttpEventKafkaLog;

import com.google.gson.Gson;

public class KafkaLogHandlerUtil {
		
	public static final String GIF = "image/gif";
	public static final String HEADER_CONNECTION_CLOSE = "Close";
	
	public final static void trackingResponse(final HttpServerRequest req) {
		String BASE64_GIF_BLANK = "R0lGODlhAQABAIAAAAAAAAAAACH5BAEAAAAALAAAAAABAAEAAAICRAEAOw==";	
		Buffer buffer = new Buffer(Base64.decode(BASE64_GIF_BLANK));
		MultiMap headers = req.response().headers();
		headers.set(CONTENT_TYPE, GIF);
		headers.set(CONTENT_LENGTH, String.valueOf(buffer.length()));
		headers.set(CONNECTION, HEADER_CONNECTION_CLOSE);
		req.response().end(buffer);
	}

	public static String generateUUID(MultiMap headers) {
		String userAgent = headers.get(USER_AGENT);
		String logDetails = headers.get(io.netty.handler.codec.http.HttpHeaders.Names.HOST);
		String result = SecurityUtil.sha1(userAgent + logDetails + System.currentTimeMillis());
		return result;
	}
	
	public static void logHttpRequestToKafka(final HttpServerRequest req, String producerKey){
		trackingResponse(req);
		HttpEventKafkaHandler kafkaHandler = HttpEventKafkaHandler.loadHandler(producerKey);
		if (kafkaHandler != null) {
			kafkaHandler.writeLogToKafka(req);
		} else {
			System.err.println("No KafkaLogHandler found for " + producerKey);
		}		
	}
	
	public static void logDataToKafka(HttpServerRequest req, String json){
		KafkaLogHandlerUtil.trackingResponse(req);
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
