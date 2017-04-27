package server.http.handler.kafka;

import java.util.Map;

import io.vertx.core.http.HttpServerRequest;
import rfx.core.util.StringUtil;
import server.http.configs.KafkaProducerConfigs;
import server.http.handler.BaseHttpHandler;
import server.http.util.KafkaLogHandlerUtil;

public class KafkaHttpEventLogHandler implements BaseHttpHandler {
	
	private static final String KAFKA_PRODUCER_NAME = "kp";
	private static final String LOG = "/l";
	protected static Map<String, Map<String, String>> kafkaProducerConfigs = KafkaProducerConfigs.load().getKafkaProducerList();

	@Override
	public void handle(final HttpServerRequest req) {
		String kp = StringUtil.safeString(req.params().get(KAFKA_PRODUCER_NAME));
		if(kafkaProducerConfigs.get(kp) != null){
			//log request to Kafka 		
			KafkaLogHandlerUtil.logAndResponseImage1px(req, kp);			
		} 
	}

	@Override
	public String getPathKey() {
		return LOG;
	}
}