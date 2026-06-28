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
 * ============================================================================
 * RedisClientFactory
 * ============================================================================
 *
 * Central factory for creating and reusing Redis clients.
 *
 * Features
 * --------
 * ✓ Jedis 7.x compatible
 * ✓ Redis 6 ACL (username/password)
 * ✓ TLS / SSL
 * ✓ Thread-safe lazy initialization
 * ✓ Connection pooling
 * ✓ Graceful shutdown
 *
 * Two client types are supported:
 *
 * 1. JedisPool
 * - configurable pool
 * - recommended for production
 *
 * 2. JedisPooled
 * - lightweight wrapper
 * - uses an internal pool
 * - suitable for most applications
 *
 * ============================================================================
 */
public final class RedisClientFactory {

    private RedisClientFactory() {
    }

    /**
     * Shared Jedis pools.
     */
    private static final Map<String, JedisPool> JEDIS_POOLS = new ConcurrentHashMap<>();

    /**
     * Shared JedisPooled instances.
     */
    private static final Map<String, JedisPooled> JEDIS_POOLED = new ConcurrentHashMap<>();

    /**
     * Returns (or lazily creates) a JedisPool.
     */
    public static JedisPool buildRedisPool(String redisPoolKey) {

        return JEDIS_POOLS.computeIfAbsent(redisPoolKey,
                RedisClientFactory::createJedisPool);
    }

    /**
     * Returns a Redis connection from the pool.
     *
     * IMPORTANT:
     *
     * <pre>
     * try(Jedis jedis = RedisClientFactory.getConnection(...)){
     *     ...
     * }
     * </pre>
     */
    public static Jedis getConnection(String redisPoolKey) {
        return buildRedisPool(redisPoolKey).getResource();
    }

    /**
     * Returns (or lazily creates) a shared JedisPooled instance.
     */
    public static JedisPooled buildRedisPooled(String redisPoolKey) {

        return JEDIS_POOLED.computeIfAbsent(redisPoolKey,
                RedisClientFactory::createJedisPooled);
    }

    /**
     * Actually creates a JedisPool.
     */
    private static JedisPool createJedisPool(String redisPoolKey) {

        RedisInfo info = getRedisInfo(redisPoolKey);

        RedisConnectionPoolConfig cfg = RedisConnectionPoolConfig.theInstance();

        JedisPoolConfig poolConfig = cfg.createJedisPoolConfig();

        DefaultJedisClientConfig clientConfig = buildClientConfig(info, cfg.getConnectionTimeout());

        return new JedisPool(
                poolConfig,
                new HostAndPort(info.getHost(), info.getPort()),
                clientConfig);
    }

    /**
     * Actually creates a JedisPooled.
     */
    private static JedisPooled createJedisPooled(String redisPoolKey) {

        RedisInfo info = getRedisInfo(redisPoolKey);

        int timeout = RedisConnectionPoolConfig.theInstance()
                .getConnectionTimeout();

        DefaultJedisClientConfig clientConfig = buildClientConfig(info, timeout);

        return new JedisPooled(
                new HostAndPort(info.getHost(), info.getPort()),
                clientConfig);
    }

    /**
     * Build Jedis client configuration.
     *
     * Supports:
     *
     * - Redis ACL
     * - Password authentication
     * - TLS
     * - Connection timeout
     * - Socket timeout
     */
    private static DefaultJedisClientConfig buildClientConfig(
            RedisInfo info,
            int timeout) {

        if (timeout <= 0) {
            timeout = 2000;
        }

        DefaultJedisClientConfig.Builder builder = DefaultJedisClientConfig.builder()
                .connectionTimeoutMillis(timeout)
                .socketTimeoutMillis(timeout)
                .ssl(info.isUseSsl());

        if (StringUtil.isNotEmpty(info.getUsername())) {
            builder.user(info.getUsername());
        }

        if (StringUtil.isNotEmpty(info.getPassword())) {
            builder.password(info.getPassword());
        }

        return builder.build();
    }

    /**
     * Load Redis configuration.
     */
    private static RedisInfo getRedisInfo(String redisPoolKey) {

        RedisInfo info = RedisConfigs.load().get(redisPoolKey);

        if (info == null) {
            throw new IllegalArgumentException(
                    "Redis configuration not found: " + redisPoolKey);
        }

        return info;
    }

    /**
     * Close a single Redis pool.
     */
    public static void close(String redisPoolKey) {

        JedisPool pool = JEDIS_POOLS.remove(redisPoolKey);

        if (pool != null && !pool.isClosed()) {
            pool.close();
        }

        /*
         * JedisPooled has a close() method.
         * Closing it releases its internal connection pool.
         */
        JedisPooled pooled = JEDIS_POOLED.remove(redisPoolKey);

        if (pooled != null) {
            pooled.close();
        }
    }

    /**
     * Close every Redis connection.
     *
     * Call once during application shutdown.
     */
    public static void shutdown() {

        JEDIS_POOLS.keySet().forEach(RedisClientFactory::close);
        JEDIS_POOLED.keySet().forEach(RedisClientFactory::close);

        JEDIS_POOLS.clear();
        JEDIS_POOLED.clear();
    }
}