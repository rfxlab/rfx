package rfx.core.configs;

import rfx.core.annotation.AutoInjectedConfig;
import rfx.core.configs.loader.Configurable;
import rfx.core.configs.loader.ParseConfigHandler;
import rfx.core.configs.loader.ParseXmlObjectHandler;
import rfx.core.nosql.jedis.RedisInfo;

public class ClusterInfoConfigs implements Configurable {
	
	@AutoInjectedConfig(injectable = true)
	static ClusterInfoConfigs instance;
		
	
	String systemEventDbPath;
	
	String masterHostname;
	int masterHttpPort;
	int masterWebSocketPort;
	
	static RedisPoolConfigs redisPoolConfigs = RedisPoolConfigs.load();
	
	/**
	 * centralized data for cluster: workers, schedule-jobs, real-time monitor, pubsub
	 * 
	 * @return RedisInfo
	 */
	public RedisInfo getClusterInfoRedis() {
		return redisPoolConfigs.get("clusterInfoRedis");
	}
	
	public String getSystemEventDbPath() {
		return systemEventDbPath;
	}
	
	public String getMasterHostname() {
		return masterHostname;
	}

	public int getMasterHttpPort() {
		return masterHttpPort;
	}

	public int getMasterWebSocketPort() {
		return masterWebSocketPort;
	}

	@Override
	public ParseConfigHandler getParseConfigHandler() {		
		return new ParseXmlObjectHandler() {		
			@Override
			public void injectFieldValue() {								
				try {
					//System.out.println(field.getType().getSimpleName());
					String str = xmlNode.select(field.getName()).text();	
					if(field.getType().getSimpleName().equals("String")){
						field.set(configurableObj, str);
					} else if(field.getType().getSimpleName().equals("int")){
						field.set(configurableObj, Integer.parseInt(str));
					}
					
				} catch (Exception e) {
					e.printStackTrace();
				}		
			}
		};
	}

	public static ClusterInfoConfigs load() {
		return instance;
	}


}
