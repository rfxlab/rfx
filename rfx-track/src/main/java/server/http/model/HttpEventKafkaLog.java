package server.http.model;

import java.io.Serializable;

import com.google.gson.Gson;


/**
 * HTTP Log Event Message
 * 
 * @author trieunt
 *
 */
public class HttpEventKafkaLog implements Serializable {
	
	private static final long serialVersionUID = -7519235169451881986L;
	
	String ip;
	String userAgent;
	String logDetails;
	String cookieString;
	String eventType;
	boolean processed = false;
	
	public HttpEventKafkaLog() {
	
	}	
	
	public HttpEventKafkaLog(boolean processed) {
		super();
		this.processed = processed;
	}
	
	public HttpEventKafkaLog(String ip, String userAgent, String logDetails,
			String cookieString, String eventType) {
		super();
		this.ip = ip;
		this.userAgent = userAgent;
		this.logDetails = logDetails;
		this.cookieString = cookieString;
		this.eventType = eventType;
	}
	
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public String getUserAgent() {
		return userAgent;
	}
	public void setUserAgent(String userAgent) {
		this.userAgent = userAgent;
	}
	public String getLogDetails() {
		return logDetails;
	}
	public void setLogDetails(String logDetails) {
		this.logDetails = logDetails;
	}
	public String getCookieString() {
		return cookieString;
	}
	public void setCookieString(String cookieString) {
		this.cookieString = cookieString;
	}
	public String getEventType() {
		return eventType;
	}
	public void setEventType(String eventType) {
		this.eventType = eventType;
	}

	public boolean isProcessed() {
		return processed;
	}

	public void setProcessed(boolean processed) {
		this.processed = processed;
	}	
	
	@Override
	public String toString() {
		return new Gson().toJson(this);
	}
}
