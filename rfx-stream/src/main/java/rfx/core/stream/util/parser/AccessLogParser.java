package rfx.core.stream.util.parser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.gson.Gson;

public class AccessLogParser {	
	public static final int NUM_FIELDS = 9;
	public static final String logEntryPattern = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\"";

	public static class DataEntry {
		String ip;
		String dateTime;
		String request;
		String response;
		long sentBytes;
		String referer;
		String userAgent;	
		
		protected DataEntry(String logEntry) {
			Pattern p = Pattern.compile(logEntryPattern);
			Matcher matcher = p.matcher(logEntry);
			if (!matcher.matches() || NUM_FIELDS != matcher.groupCount()) {
				System.err.println("Bad log entry (or problem with RE?):");
				System.err.println(logEntry);
				return;
			}		
			ip = matcher.group(1);
			dateTime = matcher.group(4);
			request = matcher.group(5);
			response = matcher.group(6);
			sentBytes = Long.parseLong(matcher.group(7));
			referer = matcher.group(8);
			userAgent = matcher.group(9);
		}
		
		public String getIp() {
			return ip;
		}
		public void setIp(String ip) {
			this.ip = ip;
		}
		public String getDateTime() {
			return dateTime;
		}
		public void setDateTime(String dateTime) {
			this.dateTime = dateTime;
		}
		public String getRequest() {
			return request;
		}
		public void setRequest(String request) {
			this.request = request;
		}

		public String getResponse() {
			return response;
		}

		public void setResponse(String response) {
			this.response = response;
		}

		public long getSentBytes() {
			return sentBytes;
		}

		public void setSentBytes(long sentBytes) {
			this.sentBytes = sentBytes;
		}

		public String getReferer() {
			return referer;
		}

		public void setReferer(String referer) {
			this.referer = referer;
		}

		public String getUserAgent() {
			return userAgent;
		}

		public void setUserAgent(String userAgent) {
			this.userAgent = userAgent;
		}
		
		@Override
		public String toString() {
			return new Gson().toJson(this);
		}
	}

	public static DataEntry parse(String logEntryLine) {
		return new DataEntry(logEntryLine);
	}
		
	
	static String logEntryLine = "85.22.92.225 - - [14/Aug/2014:19:15:36 +0700] \"GET /blog/wp-content/themes/rtpanel/assets/fontello/css/animation.css HTTP/1.1\" 200 1857 \"http://nguyentantrieu.info/blog/big-data-processing-using-plain-java/\" \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.125 Safari/537.36\"";
	public static void main(String argv[]) {		
		DataEntry data = AccessLogParser.parse(logEntryLine);
		System.out.println(data);
	
	}
}
