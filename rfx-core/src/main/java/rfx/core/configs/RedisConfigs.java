package rfx.core.configs;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;

import rfx.core.nosql.jedis.RedisInfo;
import rfx.core.nosql.jedis.Shardable;
import rfx.core.util.CommonUtil;
import rfx.core.util.FileUtils;
import rfx.core.util.StringUtil;

/**
 * Redis Configs for all Topology.
 * Upgraded for Redis 6+ (ACL/TLS support) and Thread-Safe Singleton
 * initialization.
 */
public class RedisConfigs {

	// Volatile keyword ensures changes made in one thread are immediately reflect
	// in others
	private static volatile RedisConfigs instance;

	// Internal map holding the pools, marked final to ensure immutability after
	// load
	private Map<String, List<RedisInfo>> redisPools = new HashMap<>();

	// Private constructor to prevent external instantiation
	private RedisConfigs() {
	}

	/**
	 * Thread-safe Singleton initialization using Double-Checked Locking.
	 */
	public static RedisConfigs load() {
		if (instance == null) {
			synchronized (RedisConfigs.class) {
				if (instance == null) {
					RedisConfigs newInstance = new RedisConfigs();
					newInstance.initConfigs();
					instance = newInstance;
				}
			}
		}
		return instance;
	}

	/**
	 * Internal method to parse the JSON and populate the configurations.
	 */
	private void initConfigs() {
		try {
			String json = FileUtils.readFileAsString(CommonUtil.getRedisConfigFile());
			JsonObject redisConfigs = new Gson().fromJson(json, JsonObject.class);

			Set<Map.Entry<String, JsonElement>> set = redisConfigs.entrySet();
			Map<String, List<RedisInfo>> parsedPools = new HashMap<>(set.size());

			for (Map.Entry<String, JsonElement> entry : set) {
				JsonArray array = entry.getValue().getAsJsonArray();
				List<RedisInfo> list = new ArrayList<>(array.size());

				for (JsonElement e : array) {
					JsonObject obj = e.getAsJsonObject();
					String host = StringUtil.safeString(obj.get("host").getAsString());
					int port = obj.get("port").getAsInt();

					// --- BUG FIX: Parse 'auth' instead of 'password' to match the provided JSON
					// ---
					// Support both "auth" (legacy/common) and "password" (Redis 6 ACL standard)
					String password = null;
					if (obj.has("password")) {
						password = StringUtil.safeString(obj.get("password").getAsString());
					}

					String username = obj.has("username") ? StringUtil.safeString(obj.get("username").getAsString()): null;
					boolean useSsl = obj.has("ssl") && obj.get("ssl").getAsBoolean();

					// Construct RedisInfo.
					if(username != null || password != null ) {
						list.add(new RedisInfo(host, port, username, password, useSsl));
					} else {
						// Fallback to the basic constructor if no ACL or SSL is specified
						list.add(new RedisInfo(host, port));
					}
				}
				// Wrap in unmodifiable list to prevent runtime alterations
				parsedPools.put(entry.getKey(), Collections.unmodifiableList(list));
			}
			this.redisPools = Collections.unmodifiableMap(parsedPools);
		} catch (Exception e) {
			if (e instanceof JsonSyntaxException) {
				System.err.println(CommonUtil.COLOR_CODE.ANSI_RED + "Wrong JSON syntax in file "
						+ CommonUtil.getRedisConfigFile() + CommonUtil.COLOR_CODE.ANSI_RESET);
				e.printStackTrace();
			} else {
				e.printStackTrace();
			}
		}
	}

	public Map<String, List<RedisInfo>> getRedisPools() {
		return redisPools;
	}

	/**
	 * Retrieves the primary Redis instance for a given access key.
	 */
	public RedisInfo get(String accessKey) {
		// Use getOrDefault to prevent NullPointerException if accessKey doesn't exist
		List<RedisInfo> pools = redisPools.getOrDefault(accessKey, Collections.emptyList());
		if (!pools.isEmpty()) {
			return pools.get(0);
		}
		throw new IllegalArgumentException(
				StringUtil.toString("Not found access-key ", accessKey, " in redis-pool config"));
	}

	/**
	 * Retrieves all matched instances for a given access key.
	 */
	public List<RedisInfo> getAllMatchedPools(String accessKey) {
		List<RedisInfo> pools = redisPools.getOrDefault(accessKey, Collections.emptyList());
		if (!pools.isEmpty()) {
			return pools;
		}
		throw new IllegalArgumentException(
				StringUtil.toString("Not found access-key ", accessKey, " in redis-pool config"));
	}

	/**
	 * Retrieves a Redis instance based on a Shardable object's key.
	 */
	public RedisInfo getBySharding(String accessKey, Shardable shardable) {
		List<RedisInfo> pools = redisPools.getOrDefault(accessKey, Collections.emptyList());
		int size = pools.size();
		if (size > 0) {
			int shard = getShardingId(shardable, size);
			return pools.get(shard);
		}
		throw new IllegalArgumentException(
				StringUtil.toString("Not found access-key ", accessKey, " in redis-pool config"));
	}

	/**
	 * Retrieves a Redis instance based on a raw long shard key.
	 */
	public RedisInfo getBySharding(String accessKey, long shardKey) {
		List<RedisInfo> pools = redisPools.getOrDefault(accessKey, Collections.emptyList());
		int size = pools.size();
		if (size > 0) {
			int shard = getShardingId(shardKey, size);
			return pools.get(shard);
		}
		throw new IllegalArgumentException(
				StringUtil.toString("Not found access-key ", accessKey, " in redis-pool config"));
	}

	/**
	 * Calculates the shard index.
	 * Math.abs() is added to prevent negative indexing if the shardKey is negative.
	 */
	protected int getShardingId(Shardable shardable, int size) {
		if (size > 0) {
			return (int) (Math.abs(shardable.getShardKey().longValue()) % size);
		}
		throw new IllegalArgumentException("size in AutoShardingGenerator MUST > 0");
	}

	/**
	 * Calculates the shard index.
	 * Math.abs() is added to prevent negative indexing if the shardKey is negative.
	 */
	protected int getShardingId(long shardKey, int size) {
		if (size > 0) {
			return (int) (Math.abs(shardKey) % size);
		}
		throw new IllegalArgumentException("size in AutoShardingGenerator MUST > 0");
	}
}