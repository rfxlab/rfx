package rfx.core.nosql.jedis;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPooled;
import rfx.core.configs.RedisConfigs;
import rfx.core.configs.RedisConnectionPoolConfig;
import rfx.core.util.StringUtil;

/**
 * Unified Redis client factory for LEO CDP
 * Supports both JedisPool (advanced) and JedisPooled (simple).
 *
 * Compatible with Jedis 7.x
 */
public class RedisClientFactory {

    private static Map<String,JedisPool> jedisPoolMap = new ConcurrentHashMap<>();
    private static Map<String,JedisPooled> jedisPooledMap = new ConcurrentHashMap<>();

    /**
     * Build or reuse a JedisPool with full config control.
     */
    public static synchronized JedisPool buildRedisPool(String redisPoolKey) {
    	JedisPool jedisPool = jedisPoolMap.get(redisPoolKey);
        if (jedisPool != null) {
            return jedisPool;
        }

        RedisInfo redisInfo = RedisConfigs.load().get(redisPoolKey);
        if (redisInfo == null) {
            throw new IllegalStateException("Missing Redis configuration for redisPoolKey " + redisPoolKey);
        }

        String host = redisInfo.getHost();
        int port = redisInfo.getPort();
        String auth = redisInfo.getAuth();

        RedisConnectionPoolConfig poolConfig = RedisConnectionPoolConfig.theInstance();
        JedisPoolConfig jedisPoolCfg = poolConfig.createJedisPoolConfig();

        int timeout = poolConfig.getConnectionTimeout() > 0
                ? poolConfig.getConnectionTimeout()
                : 2000;

        // Jedis 7.x pool constructor
        if (StringUtil.isNotEmpty(auth)) {
            jedisPool = new JedisPool(jedisPoolCfg, host, port, timeout, auth);
        } else {
            jedisPool = new JedisPool(jedisPoolCfg, host, port, timeout);
        }

        return jedisPool;
    }

    /**
     * Get Jedis connection from pool (remember to close it!)
     */
    public static Jedis getConnection(String redisPoolKey) {
    	return buildRedisPool(redisPoolKey).getResource();
    }

    /**
     * Create or reuse a simple JedisPooled instance.
     *
     * This uses the same Redis configuration but internal default pool settings.
     * For dev or low-load environments.
     */
    public static synchronized JedisPooled buildRedisPooled(String redisPoolKey) {
    	JedisPooled jedisPooled = jedisPooledMap.get(redisPoolKey);
        if (jedisPooled != null) {
            return jedisPooled;
        }

        RedisInfo redisInfo = RedisConfigs.load().get(redisPoolKey);
        if (redisInfo == null) {
            throw new IllegalStateException("Missing Redis configuration: clusterInfoRedis");
        }

        String host = redisInfo.getHost();
        int port = redisInfo.getPort();
        String auth = redisInfo.getAuth();

        int timeout = RedisConnectionPoolConfig.theInstance().getConnectionTimeout();
        if (timeout <= 0) timeout = 2000;

        DefaultJedisClientConfig.Builder cfg = DefaultJedisClientConfig.builder()
                .connectionTimeoutMillis(timeout)
                .socketTimeoutMillis(timeout);

        if (StringUtil.isNotEmpty(auth)) {
            cfg.password(auth);
        }

        jedisPooled = new JedisPooled(new HostAndPort(host, port), cfg.build());
        return jedisPooled;
    }

    /**
     * Gracefully close pool connections (for shutdown)
     */
    public static synchronized void close(String redisPoolKey) {
    	JedisPool jedisPool = jedisPoolMap.get(redisPoolKey);
    	if (jedisPool != null && !jedisPool.isClosed()) {
            jedisPool.close();
        }

    }
}
