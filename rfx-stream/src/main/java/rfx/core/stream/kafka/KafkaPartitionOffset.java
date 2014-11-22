package rfx.core.stream.kafka;

import java.io.Serializable;

import com.google.gson.Gson;

public class KafkaPartitionOffset implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1886938610176821611L;
	int partitionId;
	long offset;
	String topic;
	long time;
	
	public KafkaPartitionOffset(int partitionId, long offset, String topic) {
		super();
		this.partitionId = partitionId;
		this.offset = offset;
		this.topic = topic;		
	}
	
	public int getPartitionId() {
		return partitionId;
	}
	public void setPartitionId(int partitionId) {
		this.partitionId = partitionId;
	}
	public long getOffset() {
		return offset;
	}
	public void setOffset(long offset) {
		this.offset = offset;
	}
	public String getTopic() {
		return topic;
	}
	public void setTopic(String topic) {
		this.topic = topic;
	}
	public long getTime() {
		return time;
	}
	public void setTime(long time) {
		this.time = time;
	}
	
	@Override
	public String toString() {
		return new Gson().toJson(this);
	}
	
}
