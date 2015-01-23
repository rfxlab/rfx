package server.http.handler.kafka;

import java.io.Serializable;

import rfx.core.util.CharPool;
import server.kafka.EventData;

public class HttpDataLog extends EventData implements Serializable{

	private static final long serialVersionUID = -608716019415138628L;
	long unixTimeInQueue;
	String ip;
	String userAgent;
	String logDetails;
	String cookieString;
	
	public HttpDataLog(String ip, String userAgent, String logDetails,String cookieString) {
		super();				
		this.ip = ip;
		this.userAgent = userAgent;
		this.logDetails = logDetails;
		this.cookieString = cookieString;
	}
	
	@Override
	public String toString() {		
		StringBuilder logLine = new StringBuilder();
		char tab = CharPool.TAB;
		logLine.append(getIp()).append(tab);
		logLine.append(getUnixTime()).append(tab);
		logLine.append(getUserAgent()).append(tab);
		logLine.append(getLogDetails()).append(tab);
		logLine.append(getCookieString());
		return logLine.toString();
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

	public long getUnixTimeInQueue() {
		return unixTimeInQueue;
	}

	public void setUnixTimeInQueue(long unixTimeInQueue) {
		this.unixTimeInQueue = unixTimeInQueue;
	}

	@Override
	public String toStringMessage() {		
		return toString();
	}		

}
