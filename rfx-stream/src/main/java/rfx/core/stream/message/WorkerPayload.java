package rfx.core.stream.message;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class WorkerPayload<T> implements Serializable{
	static final public String SHUTDOWN = "shutdown";
	static final public String SHUTDOWN_ALL_WORKERS = "shutdown-all-workers";
	
	private static final long serialVersionUID = -565225821869202061L;
	List<T> messages;
	String topic;
	protected String cmd;
	int maxBatchSize = 1000;
	
	public WorkerPayload(String cmd) {
		super();
		this.cmd = cmd;
	}
	
	public WorkerPayload(String cmd, int maxBatchSize) {
		super();
		this.cmd = cmd;
		this.maxBatchSize = maxBatchSize;
		this.messages = new ArrayList<>(this.maxBatchSize);
	}
	
	public WorkerPayload( int maxBatchSize) {
		super();
		this.cmd = "";
		this.maxBatchSize = maxBatchSize;
		this.messages = new ArrayList<>(this.maxBatchSize);
	}

	public T getMessage(int i) {
		if(this.messages == null){
			return null;
		}
		return messages.get(i);
	}
	
	public List<T> getMessages() {
		if(this.messages == null){
			this.messages = new ArrayList<T>(0);
		}
		return messages;
	}

	public void addMessage(T message) {
		if(this.messages == null){
			this.messages = new ArrayList<T>(maxBatchSize);
		}
		this.messages.add(message);
	}
	
	public int getMaxBatchSize() {
		return maxBatchSize;
	}
	
	public boolean isEmpty(){
		if(this.messages == null){
			return true;
		}
		return this.messages.size() == 0;
	}
	
	public int size(){
		if(this.messages == null){
			return 0;
		}
		return this.messages.size();
	}
		
	public boolean isReadyFlushData(){
		if(this.messages == null){
			return false;
		}
		return messages.size() >= maxBatchSize;
	}

	
	public String getTopic() {
	    return topic;
	}

	public void setTopic(String topic) {
	    this.topic = topic;
	}
	

	public String getCmd() {
		if(cmd == null){
			cmd = "";
		}
	    return cmd;
	}

	public void setCmd(String cmd) {
	    this.cmd = cmd;
	}
	
	@Override
	public String toString() {
		return cmd;
	}
}