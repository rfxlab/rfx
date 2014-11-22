package rfx.core.stream.kafka;

import java.io.Serializable;

import rfx.core.util.StringPool;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

public class KafkaPersistentInfo implements Serializable {

	private static final long serialVersionUID = -8442770437906256872L;
	String clientName;
	String topic;	
	String hostName = StringPool.BLANK;
	int port;
	
	long readOffset = -1;
	int partitionId = -1;
	long totalReadMessage;
	long whichtime = kafka.api.OffsetRequest.EarliestTime();
		
	public KafkaPersistentInfo() {
		// TODO Auto-generated constructor stub
	}
	
	public KafkaPersistentInfo(String topic) {
		super();		
		this.topic = topic;				
	}
		
	public KafkaPersistentInfo(String topic,String hostName, int port) {
		super();		
		this.topic = topic;		
		this.hostName = hostName;
		this.port = port;
	}

	public String getClientName() {
		if(clientName == null){
			clientName = "Client_" + this.topic  + StringPool.MINUS + this.partitionId;
		}
		return clientName;
	}
	public void setClientName(String clientName) {
		this.clientName = clientName;
	}
	public int getPartitionId() {
		return partitionId;
	}
	public void setPartitionId(int partitionId) {
		this.partitionId = partitionId;
	}
	public String getHostName() {
		return hostName;
	}
	public void setHostName(String hostName) {
		this.hostName = hostName;
	}
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
	
	public long getReadOffset() {
		return readOffset;
	}

	public void setReadOffset(long readOffset) {
		this.readOffset = readOffset;
	}

	public long getTotalReadMessage() {
		return totalReadMessage;
	}
	public void increaseTotalReadMessage(){
		totalReadMessage++;
	}
	public void setTotalReadMessage(long totalReadMessage) {
		this.totalReadMessage = totalReadMessage;
	}
	
	
	public long getWhichtime() {
		return whichtime;
	}

	public void setWhichtime(long whichtime) {
		this.whichtime = whichtime;
	}

	public String getTopic() {
		return topic;
	}


	public void setTopic(String topic) {
		this.topic = topic;
	}

	public static final KafkaPersistentInfo fromJson(String json) {
		KafkaPersistentInfo _instance = null;		
		try {				
			_instance = new Gson().fromJson(json, KafkaPersistentInfo.class);			
		} catch (JsonSyntaxException e) {		
			e.printStackTrace();
			System.err.println("jsonDecode fail: \n" + json);
		}		
		return _instance;
	}
	
	
	public String toJson(){
		return new Gson().toJson(this);
	}
			
	@Override
	public String toString() {
		StringBuilder s = new StringBuilder();
		s.append("{KafkaWorkerInfo:");
		s.append(this.hostName).append(";");
		s.append(this.port).append(";");
		s.append(this.topic).append(";");
		s.append(this.partitionId).append("}");		
		return s.toString();
	}
	
}
