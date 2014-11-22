package rfx.core.stream.model;

/**
 * Kafka Task Definition for worker
 * 
 * @author trieunt
 *
 */
public class KafkaTaskDef implements TaskDef{
	String topic;
	String classPath;
	int beginPartitionId, endPartitionId;
	boolean autoStart = true;
	
	
	public KafkaTaskDef(String topic, String classPath, int beginPartitionId,
			int endPartitionId) {
		super();
		this.topic = topic;
		this.classPath = classPath;
		this.beginPartitionId = beginPartitionId;
		this.endPartitionId = endPartitionId;
	}
	
	public KafkaTaskDef(String topic, int beginPartitionId,	int endPartitionId) {
		super();
		this.topic = topic;		
		this.beginPartitionId = beginPartitionId;
		this.endPartitionId = endPartitionId;
	}
	
	public KafkaTaskDef(String topic, Class<?> clazz, int beginPartitionId,
			int endPartitionId) {
		super();
		this.topic = topic;
		this.classPath = clazz.getName();
		this.beginPartitionId = beginPartitionId;
		this.endPartitionId = endPartitionId;
	}
	
	public KafkaTaskDef(String topic, String classPath, int beginPartitionId,
			int endPartitionId, boolean autoStart) {
		super();
		this.topic = topic;
		this.classPath = classPath;
		this.beginPartitionId = beginPartitionId;
		this.endPartitionId = endPartitionId;
		this.autoStart = autoStart;
	}

	public String getTopic() {
		return topic;
	}


	public void setTopic(String topic) {
		this.topic = topic;
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
	public boolean isAutoStart() {
		return autoStart;
	}

	@Override
	public void setAutoStart(boolean autoStart) {
		this.autoStart = autoStart;
	}

	public String getClassPath() {
		return classPath;
	}


	public void setClassPath(String classPath) {
		this.classPath = classPath;
	}
	
	
}
