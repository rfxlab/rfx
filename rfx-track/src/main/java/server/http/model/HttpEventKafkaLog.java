package server.http.model;

import java.io.Serializable;

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
	
}
