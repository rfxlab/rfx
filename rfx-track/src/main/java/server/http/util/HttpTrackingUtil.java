package server.http.util;

import static io.vertx.core.http.HttpHeaders.CONNECTION;
import static io.vertx.core.http.HttpHeaders.CONTENT_LENGTH;
import static io.vertx.core.http.HttpHeaders.CONTENT_TYPE;
import static io.vertx.core.http.HttpHeaders.USER_AGENT;

import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import rfx.core.util.SecurityUtil;

public class HttpTrackingUtil {
	public static final String GIF = "image/gif";
	public static final String HEADER_CONNECTION_CLOSE = "Close";
	public static final String BASE64_GIF_BLANK = "R0lGODlhAQABAIAAAAAAAAAAACH5BAEAAAAALAAAAAABAAEAAAICRAEAOw==";
	
	public final static void setCorsHeaders(MultiMap headers){
		headers.set("Access-Control-Allow-Origin","*");
		headers.set("Access-Control-Allow-Credentials", "true");
		headers.set("Access-Control-Allow-Methods", "POST, GET");
		headers.set("Access-Control-Allow-Headers", "origin, content-type, accept, Set-Cookie");
	}
	
	public final static void trackingResponse(final HttpServerRequest req) {			
		Buffer buffer = Buffer.buffer(BASE64_GIF_BLANK);
		HttpServerResponse response = req.response();
		MultiMap headers = response.headers();
		headers.set(CONTENT_TYPE, GIF);
		headers.set(CONTENT_LENGTH, String.valueOf(buffer.length()));
		headers.set(CONNECTION, HEADER_CONNECTION_CLOSE);
		setCorsHeaders(headers);
		response.end(buffer);
	}
	
	public static String generateUUID(MultiMap headers) {
		String userAgent = headers.get(USER_AGENT);
		String logDetails = headers.get(io.netty.handler.codec.http.HttpHeaders.Names.HOST);
		String result = SecurityUtil.sha1(userAgent + logDetails + System.currentTimeMillis());
		return result;
	}
}
