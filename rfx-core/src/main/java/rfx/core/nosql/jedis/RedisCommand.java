package rfx.core.nosql.jedis;

import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.exceptions.JedisException;
import rfx.core.util.LogUtil;

public abstract class RedisCommand<T> {

    // JedisPooled is thread-safe and handles its own internal pool
    protected final JedisPooled jedisClient;

    public RedisCommand(JedisPooled jedisClient) {
        if (jedisClient == null) {
            throw new IllegalArgumentException("jedisClient is NULL!");
        }
        this.jedisClient = jedisClient;
    }

    /**
     * Executes the Redis command safely, managing exceptions and resource scope.
     */
    public T execute() {
        try {
            return build(jedisClient);
        } catch (JedisException e) {
            LogUtil.e("Redis command failed: " + e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            LogUtil.e("Unexpected error in Redis command: " + e.getMessage(), e);
            throw new JedisException("Unexpected Redis command error", e);
        }
    }

    /**
     * Implement your Redis logic here using the provided JedisPooled client.
     */
    protected abstract T build(JedisPooled jedis) throws JedisException;
}
