package rfx.core.stream.kafka;

import java.io.Serializable;

public class KafkaData implements Serializable{

	private static final long serialVersionUID = -5231068247054720393L;
	long offset;
	String data;
		
	public KafkaData(long offset, String data) {
		super();
		this.offset = offset;
		this.data = data;
	}
	public long offset() {
		return offset;
	}
	public String data() {
		if(data == null){
			data = "";
		}
		return data;
	}
	
	@Override
	public String toString() {
		return this.offset+": "+this.data;
	}
	
}
