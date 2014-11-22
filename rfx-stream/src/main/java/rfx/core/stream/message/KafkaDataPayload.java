package rfx.core.stream.message;

import java.util.ArrayList;
import java.util.List;

import rfx.core.stream.kafka.KafkaData;

public class KafkaDataPayload {

	List<KafkaData> kafkaDataList;
	String topic;
	int partitionId;
	long beginOffset;
	long endOffset;
	
	public KafkaDataPayload(List<KafkaData> kafkaDataList, String topic, int partitionId) {
		super();
		this.kafkaDataList = kafkaDataList;
		this.topic = topic;
		this.partitionId = partitionId;
	}
	public KafkaDataPayload(List<KafkaData> kafkaDataList, String topic, int partitionId,long beginOffset,long endOffset) {
		super();
		this.kafkaDataList = kafkaDataList;
		this.topic = topic;
		this.partitionId = partitionId;
		this.beginOffset = beginOffset;
		this.endOffset = endOffset;
	}
	
	public KafkaDataPayload(String topic, int partitionId) {
		super();
		this.kafkaDataList = new ArrayList<>(0);
		this.topic = topic;
		this.partitionId = partitionId;
	}	
	
	public List<KafkaData> getKafkaDataList() {
		if(kafkaDataList == null){
			this.kafkaDataList = new ArrayList<>(0);
		}
		return kafkaDataList;
	}
	public void setKafkaDataList(List<KafkaData> kafkaDataList) {
		this.kafkaDataList = kafkaDataList;
	}
	public String getTopic() {
		return topic;
	}
	
	public int getPartitionId() {
		return partitionId;
	}
	
	public long getBeginOffset() {
		return beginOffset;
	}
	public void setBeginOffset(long beginOffset) {
		this.beginOffset = beginOffset;
	}
	public long getEndOffset() {
		return endOffset;
	}
	public void setEndOffset(long endOffset) {
		this.endOffset = endOffset;
	}
	public int size(){
		return this.kafkaDataList.size();
	}
	
}
