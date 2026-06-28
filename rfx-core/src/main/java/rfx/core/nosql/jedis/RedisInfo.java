package rfx.core.nosql.jedis;

import java.util.Objects;

/**
 * RedisInfo — modern Redis connection wrapper using JedisPooled (Jedis 7.x)
 *
 * Thread-safe, simple, and ideal for single-node or cluster-aware Redis setups.
 */
public class RedisInfo {
    public static final String LOCALHOST_STR = "localhost";

    private String host;
    private int port;
    private String username, password;
    private boolean useSsl;

    /**
     * Creates a RedisInfo object without authentication.
     */
    public RedisInfo(String host, int port) {
        this(host, port, null, null, false);
    }

    /**
     * Creates a RedisInfo object with authentication.
     */
    public RedisInfo(String host, int port, String username, String password, boolean useSsl) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.useSsl = useSsl;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public boolean isUseSsl() {
        return useSsl;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setUseSsl(boolean useSsl) {
        this.useSsl = useSsl;
    }

    /**
     * Compares equality based on normalized host and port.
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj instanceof RedisInfo) {
            RedisInfo other = (RedisInfo) obj;
            String thisHost = normalizeHost(this.host);
            String thatHost = normalizeHost(other.host);
            return this.port == other.port && thisHost.equals(thatHost);
        }
        return false;
    }

    /**
     * Generates a hash code consistent with the equals() method.
     * Essential for storing RedisInfo safely in HashMaps or HashSets.
     */
    @Override
    public int hashCode() {
        return Objects.hash(normalizeHost(this.host), this.port);
    }

    @Override
    public String toString() {
        return host + ":" + port;
    }

    /**
     * Normalizes common local hostnames to a standard string.
     */
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