package rfx.core.stream.file;

import rfx.core.model.CallbackResult;
import rfx.core.stream.message.KafkaDataPayload;

public class DataFileCallbackResult extends CallbackResult<String> {

	KafkaDataPayload kafkaDataPayload;
	
	public DataFileCallbackResult(KafkaDataPayload kafkaDataPayload) {
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
