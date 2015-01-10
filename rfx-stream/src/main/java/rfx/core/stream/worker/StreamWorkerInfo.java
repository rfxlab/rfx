package rfx.core.stream.worker;

import rfx.core.stream.graph.Vertex;
import rfx.core.util.StringUtil;

import com.google.gson.Gson;

public class StreamWorkerInfo extends Vertex{
	String host;
	int port;
	String kafkaTopic;
	int beginPartitionId  = 0;
	int endPartitionId  = 1;
	

	public StreamWorkerInfo(String id, String name, String host, int port,
			String kafkaTopic, int beginPartitionId, int endPartitionId) {		
		super(id, name);
		this.host = host;
		this.port = port;
		this.kafkaTopic = kafkaTopic;
		this.beginPartitionId = beginPartitionId;
		this.endPartitionId = endPartitionId;
	}

	public static StreamWorkerInfo create(String host, int port,
			String kafkaTopic, int beginPartitionId, int endPartitionId){
		String id = getId(host, port, kafkaTopic, beginPartitionId, endPartitionId);
		String name = StringUtil.join(":", "worker",kafkaTopic);
		return new StreamWorkerInfo(id, name, host, port, kafkaTopic, beginPartitionId, endPartitionId);
	}
	
	public static String getId(String host, int port, String kafkaTopic, int beginPartitionId, int endPartitionId){
		String id = StringUtil.join(":", "w",host,port,kafkaTopic,beginPartitionId,endPartitionId);
		//System.out.println("id "+id);
		return id;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getKafkaTopic() {
		return kafkaTopic;
	}

	public void setKafkaTopic(String kafkaTopic) {
		this.kafkaTopic = kafkaTopic;
	}

	public int getBeginPartitionId() {
		return beginPartitionId;
	}

	public void setBeginPartitionId(int beginPartitionId) {
		this.beginPartitionId = beginPartitionId;
	}

	public int getEndPartitionId() {
		return endPartitionId;
	}

	public void setEndPartitionId(int endPartitionId) {
		this.endPartitionId = endPartitionId;
	}
	@Override
	public String toString() {
		return StringUtil.toString("(",id,")");
	};
	
	public String toJson() {
		return new Gson().toJson(this);
	}
}
