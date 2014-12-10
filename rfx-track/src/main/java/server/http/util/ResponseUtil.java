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
import server.kafka.HttpLogKafkaHandler;
import server.kafka.KafkaLogHandler;

public class ResponseUtil {
	
	public static final String BASE64_GIF_BLANK = "R0lGODlhAQABAIAAAAAAAAAAACH5BAEAAAAALAAAAAABAAEAAAICRAEAOw==";
	static final byte[] DECODED_GIF_1PX_BYTES = Base64.decode(BASE64_GIF_BLANK);
	public static final String GIF = "image/gif";
	public static final String HEADER_CONNECTION_CLOSE = "Close";
	
	public final static void trackingResponse(HttpServerRequest req) {
		Buffer buffer = new Buffer(DECODED_GIF_1PX_BYTES);
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
	
	public static void logRequestToKafka(HttpServerRequest req, String kafkaType){
		KafkaLogHandler kafkaHandler = HttpLogKafkaHandler.getKafkaHandler(kafkaType);
		if (kafkaHandler != null) {
			kafkaHandler.writeLogToKafka(req);
		} else {
			System.err.println("No KafkaLogHandler found for " + kafkaType);
		}
		trackingResponse(req);
	}

}
