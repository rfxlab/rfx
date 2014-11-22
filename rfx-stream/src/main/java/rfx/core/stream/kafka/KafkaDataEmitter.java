package rfx.core.stream.kafka;

import rfx.core.model.CallbackResult;
import rfx.core.stream.emitters.DataEmitter;
import rfx.core.stream.message.KafkaDataPayload;

public class KafkaDataEmitter extends DataEmitter {
		
	public KafkaDataEmitter() {
		super();
	}

	private KafkaDataSeeder kafkaDataSeeder;	
	
	public KafkaDataEmitter(KafkaDataSeeder kafkaDataSeeder) {
		super();
		this.kafkaDataSeeder = kafkaDataSeeder;
	}	
	
	public KafkaDataSeeder myKafkaDataSeeder() {
		return kafkaDataSeeder;
	}

	public KafkaDataEmitter setKafkaDataSeeder(KafkaDataSeeder kafkaDataSeeder) {
		this.kafkaDataSeeder = kafkaDataSeeder;
		return this;
	}
	
	@Override
	public synchronized CallbackResult<String> call() {
		KafkaDataPayload dataPayload = this.myKafkaDataSeeder().buildQuery().seedData();
		//topology.counter(getMetricKey()).addAndGet(dataPayload.size());//TODO
		return new KafkaCallbackResult(dataPayload);
	}
	
	@Override
	public String getMetricKey() {
		return "emitter#"+this.myKafkaDataSeeder().getTopic()+"#"+this.myKafkaDataSeeder().getPartition();
	}
	
}
