package server.http.handler.kafka;
import static io.netty.handler.codec.http.HttpHeaders.Names.COOKIE;
import static io.netty.handler.codec.http.HttpHeaders.Names.REFERER;
import static io.netty.handler.codec.http.HttpHeaders.Names.USER_AGENT;

import java.net.URLDecoder;
import java.util.concurrent.atomic.AtomicLong;

import org.vertx.java.core.MultiMap;
import org.vertx.java.core.http.HttpServerRequest;

import rfx.core.util.HttpRequestUtil;
import rfx.core.util.LogUtil;
import rfx.core.util.StringPool;
import rfx.core.util.StringUtil;
import server.http.model.HttpEventKafkaLog;
import server.kafka.EventData;
import server.kafka.KafkaProducerHandler;

public class HttpEventKafkaHandlerImpl extends HttpEventKafkaHandler {
	
	AtomicLong counter = new AtomicLong();				
	KafkaProducerHandler kafkaProducerHandler;
		
	public void countingToDebug() {
		long c = counter.addAndGet(1);
		LogUtil.d(kafkaProducerHandler.getTopic() + " logCounter: " + c);		
	}
		
	protected HttpEventKafkaHandlerImpl(String producerKey) {
		kafkaProducerHandler = KafkaProducerHandler.getKafkaHandler(producerKey);
	}	
	
	void writeLogToKafka(String ip, String userAgent, String logDetails, String cookieString){		
		countingToDebug();
		System.out.println("cookieString "+cookieString);
		EventData data = new HttpDataLog(ip, userAgent, logDetails, cookieString);		
		if( KafkaProducerHandler.KAFKA_ENABLED){
			kafkaProducerHandler.writeData(data);
		} else {			
			LogUtil.d("Skip writeLogToKafka: "+data.toStringMessage());
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
		String queryStr = uri.substring(idx+1);
		if(StringUtil.isEmpty(queryStr)){
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
		writeLogToKafka(remoteIp, userAgent, queryStr, cookieStBuilder.toString());		
	}

	@Override
	public void writeLogToKafka(HttpEventKafkaLog el) {
		writeLogToKafka(el.getIp(), el.getUserAgent(), el.getLogDetails(), el.getCookieString());
	}	

}
