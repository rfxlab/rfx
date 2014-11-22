package rfx.core.stream.kafka;

import rfx.core.model.CallbackResult;
import rfx.core.stream.message.KafkaDataPayload;

public class KafkaCallbackResult extends CallbackResult<String> {

	KafkaDataPayload kafkaDataPayload;
	
	public KafkaCallbackResult(KafkaDataPayload kafkaDataPayload) {
		super();
		this.kafkaDataPayload = kafkaDataPayload;
	}

	public KafkaDataPayload getKafkaDataPayload() {
		return kafkaDataPayload;
	}

	public void setKafkaDataPayload(KafkaDataPayload kafkaDataPayload) {
		this.kafkaDataPayload = kafkaDataPayload;
	}
	
}
