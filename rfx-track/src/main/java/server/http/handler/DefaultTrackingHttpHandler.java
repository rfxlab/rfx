package server.http.handler;

import java.util.Map;

import org.vertx.java.core.http.HttpServerRequest;

import rfx.core.util.StringPool;
import server.http.configs.KafkaProducerConfigs;
import server.http.util.LogHandlerUtil;

public class DefaultTrackingHttpHandler extends BaseHttpHandler {
	private static final String PONG = "PONG";
	public static final String PING = "/ping";
	protected static Map<String, Map<String, String>> kafkaProducerConfigs = KafkaProducerConfigs.load().getKafkaProducerList();

	@Override
	public boolean handle(HttpServerRequest req) {
		String uri = req.uri();
		System.out.println("URI " + uri);

		String key = uri.replace("/", StringPool.BLANK);	
		//common
		if (uri.equalsIgnoreCase(PING)) {
			req.response().end(PONG);
			return true;
		}		
		else if(kafkaProducerConfigs.get(key) != null){
			//log request to Kafka 		
			LogHandlerUtil.logRequestToKafka(req, key);
			return true;
		}	
		return false;
	}

}
