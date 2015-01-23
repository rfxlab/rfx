package server.kafka;


public abstract class EventData {

	protected long unixTime;

	public EventData() {
		super();
		this.unixTime = System.currentTimeMillis() / 1000L;
	}

	public long getUnixTime() {			
		return unixTime;
	}

	public void setUnixTime(long unixTime) {
		this.unixTime = unixTime;
	}
	
	public abstract String toStringMessage();
}