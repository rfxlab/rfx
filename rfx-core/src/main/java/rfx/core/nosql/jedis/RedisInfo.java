package rfx.core.nosql.jedis;

/**
 * RedisInfo — modern Redis connection wrapper using JedisPooled (Jedis 7.x)
 *
 * Thread-safe, simple, and ideal for single-node or cluster-aware Redis setups.
 */
public class RedisInfo {
	public static final String LOCALHOST_STR = "localhost";

	private String host;
	private int port;
	private String auth;

	public RedisInfo(String host, int port) {
		this(host, port, null);
	}

	public RedisInfo(String host, int port, String auth) {
		this.host = host;
		this.port = port;
		this.auth = auth;
	}


	public String getHost() {
		return host;
	}

	public int getPort() {
		return port;
	}

	public String getAuth() {
		return auth;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public void setAuth(String auth) {
		this.auth = auth;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof RedisInfo) {
			RedisInfo other = (RedisInfo) obj;
			String thisHost = normalizeHost(this.host);
			String thatHost = normalizeHost(other.host);
			return this.port == other.port && thisHost.equals(thatHost);
		}
		return false;
	}

	@Override
	public String toString() {
		return host + ":" + port;
	}

	private String normalizeHost(String host) {
		if (host == null)
			return LOCALHOST_STR;
		switch (host) {
		case "127.0.0.1":
		case "::1":
			return LOCALHOST_STR;
		default:
			return host;
		}
	}
}
