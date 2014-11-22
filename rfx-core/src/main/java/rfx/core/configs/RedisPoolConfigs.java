package rfx.core.configs;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import rfx.core.annotation.AutoInjectedConfig;
import rfx.core.configs.loader.ConfigAutoLoader;
import rfx.core.configs.loader.Configurable;
import rfx.core.configs.loader.ParseConfigHandler;
import rfx.core.configs.loader.ParseXmlObjectHandler;
import rfx.core.nosql.jedis.RedisInfo;
import rfx.core.nosql.jedis.Shardable;
import rfx.core.util.StringUtil;

/**
 * Redis Pool Configs for all Storm Topology
 * 
 * @author trieu
 * 
 */
public class RedisPoolConfigs implements Serializable, Configurable{

	private static final long serialVersionUID = -1787016004539301832L;

	@AutoInjectedConfig(injectable = true)
	static RedisPoolConfigs instance;
	
	public static RedisPoolConfigs load() {		
		if(instance == null){
			ConfigAutoLoader.loadAll();
		}
		return instance;
	}	
	
	Map<String, List<RedisInfo>> redisPools;	
	public Map<String, List<RedisInfo>> getRedisPools() {
		return redisPools;
	}
	
	public RedisInfo get(String accessKey) {
		List<RedisInfo> pools = redisPools.get(accessKey);
		if(pools.size()>0){
			return pools.get(0);
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
	

	@Override
	public ParseConfigHandler getParseConfigHandler() {		 
		ParseConfigHandler fieldHandler = new ParseXmlObjectHandler() {
			@Override
			public void injectFieldValue() {
				try {
					
					String fieldname = field.getName();
					if("java.util.Map".equals(field.getType().getName())){
						Elements nodes = xmlNode.select("*[fieldname="+fieldname+"]");						
						if(nodes.size() >= 1){							
							Elements childs = nodes.get(0).children();							
							Map<String, List<RedisInfo>> redisPools = new HashMap<>();
							for (Element child : childs) {
								String host = StringUtil.safeString(child.attr("host"),"localhost").trim();
								int port = StringUtil.safeParseInt(child.attr("port"),6379);
								String auth = StringUtil.safeString(child.attr("auth"),"").trim();
								
								RedisInfo redisInfo;
								if( auth.isEmpty()){
									redisInfo = new RedisInfo(host, port);
								} else {
									redisInfo = new RedisInfo(host, port, auth);
								}								
								Elements accesskeys = child.children();
								
								for (Element key : accesskeys) {
									String keyname = key.text();
									List<RedisInfo> redisInfos = redisPools.get(keyname);
									if(redisInfos == null){
										redisInfos = new ArrayList<>();
										redisPools.put(keyname, redisInfos);
									} 
									redisInfos.add(redisInfo);									
								}
							}
							field.set(configurableObj, Collections.unmodifiableMap(redisPools));
						}
					}					
				} catch (Exception e) {					
					e.printStackTrace();
				}
			}
		};
		return fieldHandler;
	}
}
