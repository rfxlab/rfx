package rfx.core.nosql.jedis;

import java.util.concurrent.*;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Vertx;
import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisException;
import rfx.core.stream.cluster.ClusterDataManager;

/**
 * High-concurrency Redis command executor with CPU-aware thread pool
 * and automatic resource cleanup.
 *
 * @param <T> Result type
 * @author Trieu
 * @since 2025
 */
public abstract class RedisCommand<T> {

    private static final Logger logger = LoggerFactory.getLogger(RedisCommand.class);

    private final JedisPool jedisPool;

    // Dynamically size thread pool based on CPU cores
    private static final int CORES = Runtime.getRuntime().availableProcessors();
    private static final ExecutorService REDIS_EXECUTOR = new ThreadPoolExecutor(
            CORES,                      // core threads = CPU cores
            CORES * 4,                  // allow bursts up to 4× cores
            60L, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(2000), // bounded queue for backpressure
            new ThreadFactory() {
                private final ThreadFactory base = Executors.defaultThreadFactory();
                @Override public Thread newThread(Runnable r) {
                    Thread t = base.newThread(r);
                    t.setName("redis-io-" + t.getId());
                    t.setDaemon(true);
                    return t;
                }
            },
            new ThreadPoolExecutor.CallerRunsPolicy() // throttles callers under pressure
    );

    public RedisCommand(JedisPool jedisPool) {
        if (jedisPool == null)
            throw new IllegalArgumentException("jedisPool is NULL!");
        this.jedisPool = jedisPool;
    }

    /**
     * Blocking execution. Do NOT call from Vert.x event loop.
     */
    public T execute() {
        try (Jedis jedis = jedisPool.getResource()) {
            return build(jedis);
        } catch (Exception e) {
            logger.error("Redis command failed: {}", e.getMessage(), e);
            throw new JedisException("Redis command failed", e);
        }
    }

    /**
     * High-throughput async execution on dedicated Redis I/O pool.
     */
    public CompletableFuture<T> executeAsync() {
        return CompletableFuture.supplyAsync(() -> {
            try (Jedis jedis = jedisPool.getResource()) {
                return build(jedis);
            } catch (Exception e) {
                logger.error("Redis async command failed: {}", e.getMessage(), e);
                throw new CompletionException(new JedisException("Redis async command failed", e));
            }
        }, REDIS_EXECUTOR);
    }

    /**
     * Async execution integrated with Vert.x worker threads.
     */
    public void executeAsync(Consumer<T> onSuccess, Consumer<Throwable> onError) {
        Vertx vertx = ClusterDataManager.theVertx();
        vertx.<T>executeBlocking(promise -> {
            try (Jedis jedis = jedisPool.getResource()) {
                promise.complete(build(jedis));
            } catch (Exception e) {
                promise.fail(e);
            }
        }, res -> {
            if (res.succeeded()) {
                if (onSuccess != null) onSuccess.accept(res.result());
            } else {
                Throwable cause = res.cause();
                logger.error("Redis Vert.x async failed: {}", cause.getMessage(), cause);
                if (onError != null) onError.accept(cause);
            }
        });
    }

    /**
     * Implement Redis logic here using the provided Jedis instance.
     */
    protected abstract T build(Jedis jedis) throws JedisException;

    /**
     * Graceful shutdown hook for Redis I/O pool.
     */
    public static void shutdownExecutor() {
        REDIS_EXECUTOR.shutdown();
        try {
            if (!REDIS_EXECUTOR.awaitTermination(5, TimeUnit.SECONDS)) {
                REDIS_EXECUTOR.shutdownNow();
            }
        } catch (InterruptedException ignored) {}
    }
}
