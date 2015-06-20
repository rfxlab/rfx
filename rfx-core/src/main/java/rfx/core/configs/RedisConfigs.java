package rfx.core.configs;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import rfx.core.nosql.jedis.RedisInfo;
import rfx.core.nosql.jedis.Shardable;
import rfx.core.util.CommonUtil;
import rfx.core.util.FileUtils;
import rfx.core.util.StringUtil;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;

/**
 * Redis Configs for all Topology
 * 
 * @author trieu
 * 
 */
public class RedisConfigs{

	static RedisConfigs instance;
	
	public static RedisConfigs load() {		
		if(instance == null){			
			try {
				String json = FileUtils.readFileAsString(CommonUtil.getRedisConfigFile());				
				//Type type = new TypeToken<Map<String, ArrayList<RedisInfo>>>(){}.getType(); 
				JsonObject redisConfigs = new Gson().fromJson(json, JsonObject.class);
						
				Set<Map.Entry<String, JsonElement>> set = redisConfigs.entrySet();
				Map<String, List<RedisInfo>> redisPools = new HashMap<String, List<RedisInfo>>(set.size());
				for (Entry<String, JsonElement> entry : set) {
					JsonArray array = entry.getValue().getAsJsonArray();
					List<RedisInfo> list = new ArrayList<RedisInfo>(array.size());									
					for (JsonElement e : array) {
						JsonObject obj = e.getAsJsonObject();
						String host = StringUtil.safeString(obj.get("host").getAsString());
						int port  = obj.get("port").getAsInt();
						list.add(new RedisInfo(host, port));						
					}
					redisPools.put(entry.getKey(), list );
				}
				instance = new RedisConfigs();
				instance.redisPools = redisPools;		
			}
			catch (Exception e) {
				if (e instanceof JsonSyntaxException) {
					e.printStackTrace();
					System.err.println("Wrong JSON syntax in file "+CommonUtil.getRedisPoolConnectionConfigFile());
				}
				else {
					e.printStackTrace();
				}
			}
		}
		return instance;
	}	
	
	Map<String, List<RedisInfo>> redisPools;	
	public Map<String, List<RedisInfo>> getRedisPools() {
		return redisPools;
	}
	
	public RedisInfo get(String accessKey) {
		List<RedisInfo> pools = redisPools.get(accessKey);
		if(pools != null){
			if(pools.size()>0){
				return pools.get(0);
			}
		}
		throw new IllegalArgumentException(StringUtil.toString("Not found access-key ",accessKey," in redis-pool config"));
	}
	
	public List<RedisInfo> getAllMatchedPools(String accessKey) {
		List<RedisInfo> pools = redisPools.get(accessKey);
		if(pools.size()>0){
			return pools;
		}
		throw new IllegalArgumentException(StringUtil.toString("Not found access-key ",accessKey," in redis-pool config"));
	}
	
	public RedisInfo getBySharding(String accessKey, Shardable shardable) {
		List<RedisInfo> pools = redisPools.get(accessKey);
		int size = pools.size();
		if(size>0){
			int shard = getShardingId(shardable, size);
			return pools.get(shard);
		}
		throw new IllegalArgumentException(StringUtil.toString("Not found access-key ",accessKey," in redis-pool config"));
	}
	
	public RedisInfo getBySharding(String accessKey, long shardKey ) {
		List<RedisInfo> pools = redisPools.get(accessKey);
		int size = pools.size();
		if(size>0){
			int shard = getShardingId(shardKey, size);
			return pools.get(shard);
		}
		throw new IllegalArgumentException(StringUtil.toString("Not found access-key ",accessKey," in redis-pool config"));
	}
	
	protected int getShardingId(Shardable shardable, int size){
		if(size > 0){
			return (int) (shardable.getShardKey().longValue() % size);
		}
		throw new IllegalArgumentException("size in AutoShardingGenerator MUST > 0");
	}
	
	protected int getShardingId(long shardKey, int size){
		if(size > 0){
			return (int) (shardKey % size);
		}
		throw new IllegalArgumentException("size in AutoShardingGenerator MUST > 0");
	}
	
	public static void main(String[] args) {
		RedisConfigs configs = RedisConfigs.load();
		System.out.println(configs.get("clusterInfoRedis"));
	}
	
}
