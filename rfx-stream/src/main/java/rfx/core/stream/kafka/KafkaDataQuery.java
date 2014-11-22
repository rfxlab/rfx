package rfx.core.stream.kafka;

import java.util.List;

public class KafkaDataQuery {
	
	public static interface QueryFilter {
		public boolean filter(long offset,String data);
	}

	long maxReads;
	long recentReadOffset = -1;
	String topic;
	int partition;
	List<String> seedBrokers;	
	String clientName;
	int bufferForOneFetch = 500000;//in bytes	
	QueryFilter queryFilter;
	
	public KafkaDataQuery(long recentReadOffset, String topic, int partition,List<String> seedBrokers) {
		super();
		this.recentReadOffset = recentReadOffset;
		this.topic = topic;
		this.partition = partition;
		this.seedBrokers = seedBrokers;
	}
	
	public KafkaDataQuery(String topic, int partition,List<String> seedBrokers) {
		super();		
		this.topic = topic;
		this.partition = partition;
		this.seedBrokers = seedBrokers;
	}
	
	public KafkaDataQuery(String topic, int partition) {
		super();		
		this.topic = topic;
		this.partition = partition;		
	}
	
	public KafkaDataQuery(long maxReads,long recentReadOffset, String topic, int partition,List<String> seedBrokers) {
		super();
		this.maxReads = maxReads;
		this.recentReadOffset = recentReadOffset;
		this.topic = topic;
		this.partition = partition;
		this.seedBrokers = seedBrokers;
	}
	
	public long getRecentReadOffset() {
		return recentReadOffset;
	}
	public void setRecentReadOffset(long recentReadOffset) {
		this.recentReadOffset = recentReadOffset;
	}
	public String getTopic() {
		return topic;
	}
	public void setTopic(String topic) {
		this.topic = topic;
	}
	public int getPartition() {
		return partition;
	}
	public void setPartition(int partition) {
		this.partition = partition;
	}
	public List<String> getSeedBrokers() {
		return seedBrokers;
	}
	public void setSeedBrokers(List<String> seedBrokers) {
		this.seedBrokers = seedBrokers;
	}
	
	public int getBufferForOneFetch() {
		return bufferForOneFetch;
	}

	public void setBufferForOneFetch(int bufferForOneFetch) {
		this.bufferForOneFetch = bufferForOneFetch;
	}
	
	public long getMaxReads() {
		return maxReads;
	}

	public void setMaxReads(long maxReads) {
		this.maxReads = maxReads;
	}

	public String buildClientName(){
		if(this.clientName == null){
			this.clientName = "Client_" + topic + "_" + partition;
		}
		return clientName;
	}
	
	public String buildClientName(String prefix){
		if(this.clientName == null){
			this.clientName = prefix + topic + "_" + partition;
		}
		return clientName;
	}
	
	public boolean queryFilter(long offset,String data) {
		if(queryFilter != null){
			return this.queryFilter.filter(offset, data);
		}
		//alway return true it queryFilter is NULL (accept all values)
		return true;
	}

	public void setQueryFilter(QueryFilter queryFilter) {
		this.queryFilter = queryFilter;
	}
	
}
