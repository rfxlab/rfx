package rfx.core.nosql.jedis;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.vertx.core.Vertx;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.exceptions.JedisException;
import rfx.core.util.LogUtil;

/**
 * Non-blocking Redis command executor compatible with Vert.x.
 * 
 * @param <T> Result type
 * 
 * @author Trieu
 * @since 2025
 */
public abstract class RedisCommand<T> {

    protected final JedisPooled jedis;

    public RedisCommand(JedisPooled jedisPooled) {
        if (jedisPooled == null) {
            throw new IllegalArgumentException("jedisPooled is NULL!");
        }
        this.jedis = jedisPooled;
    }

    /**
     * Synchronous execution — legacy compatibility.
     * Should NOT be called from Vert.x event loop.
     */
    public T execute() {
        try {
            return build();
        } catch (JedisException e) {
            LogUtil.e("Redis command failed: " + e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            LogUtil.e("Unexpected Redis command error: " + e.getMessage(), e);
            throw new JedisException("Unexpected Redis command error", e);
        }
    }

    /**
     * Asynchronous execution using CompletableFuture.
     * Runs the Redis command on a separate worker thread.
     */
    public CompletableFuture<T> executeAsync() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return build();
            } catch (JedisException e) {
                LogUtil.e("Redis command failed (async): " + e.getMessage(), e);
                throw e;
            } catch (Exception e) {
                LogUtil.e("Unexpected Redis command error (async): " + e.getMessage(), e);
                throw new JedisException("Unexpected Redis command error", e);
            }
        });
    }

    /**
     * Executes asynchronously using Vert.x worker threads if Vert.x is provided.
     * 
     * This is the preferred method when running inside Vert.x.
     */
    public void executeAsync(Vertx vertx, java.util.function.Consumer<T> onSuccess, java.util.function.Consumer<Throwable> onError) {
        vertx.<T>executeBlocking(promise -> {
            try {
                T result = build();
                promise.complete(result);
            } catch (Exception e) {
                promise.fail(e);
            }
        }, res -> {
            if (res.succeeded()) {
                if (onSuccess != null) onSuccess.accept(res.result());
            } else {
                LogUtil.e("Redis async (Vert.x) command failed: " + res.cause().getMessage(), res.cause());
                if (onError != null) onError.accept(res.cause());
            }
        });
    }

    /**
     * Implement your Redis logic here using the provided JedisPooled client.
     */
    protected abstract T build() throws JedisException;
}
