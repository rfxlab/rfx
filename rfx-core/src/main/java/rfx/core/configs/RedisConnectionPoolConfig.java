package rfx.core.configs;

import redis.clients.jedis.JedisPoolConfig;
import rfx.core.util.CommonUtil;
import rfx.core.util.FileUtils;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

public class RedisConnectionPoolConfig {
	
	protected static RedisConnectionPoolConfig _instance = null;
	
	public static RedisConnectionPoolConfig theInstance() {
		if(_instance == null){
			try {
				String json = FileUtils.readFileAsString(CommonUtil.getRedisPoolConnectionConfigFile());
				_instance = new Gson().fromJson(json, RedisConnectionPoolConfig.class);
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
		return _instance;
	}
	public static JedisPoolConfig getJedisPoolConfigInstance(){		
		return theInstance().getJedisPoolConfig();
	}
	
	int connectionTimeout = 0;
	int maxTotal = 20;
	int maxIdle = 10;
	int minIdle = 1;
	int maxWaitMillis = 3000;
	int numTestsPerEvictionRun = 10;
	boolean testOnBorrow = true;
	boolean testOnReturn = true;
	boolean testWhileIdle = true;
	int timeBetweenEvictionRunsMillis = 60000;
	
	public int getMaxTotal() {
		return maxTotal;
	}

	public void setMaxTotal(int maxActive) {
		this.maxTotal = maxActive;
	}

	public int getMaxIdle() {
		return maxIdle;
	}

	public void setMaxIdle(int maxIdle) {
		this.maxIdle = maxIdle;
	}

	public int getMinIdle() {
		return minIdle;
	}

	public void setMinIdle(int minIdle) {
		this.minIdle = minIdle;
	}


	public int getMaxWaitMillis() {
		return maxWaitMillis;
	}
	public void setMaxWaitMillis(int maxWaitMillis) {
		this.maxWaitMillis = maxWaitMillis;
	}
	public int getNumTestsPerEvictionRun() {
		return numTestsPerEvictionRun;
	}

	public void setNumTestsPerEvictionRun(int numTestsPerEvictionRun) {
		this.numTestsPerEvictionRun = numTestsPerEvictionRun;
	}

	public boolean isTestOnBorrow() {
		return testOnBorrow;
	}

	public void setTestOnBorrow(boolean testOnBorrow) {
		this.testOnBorrow = testOnBorrow;
	}

	public boolean isTestOnReturn() {
		return testOnReturn;
	}

	public void setTestOnReturn(boolean testOnReturn) {
		this.testOnReturn = testOnReturn;
	}

	public boolean isTestWhileIdle() {
		return testWhileIdle;
	}

	public void setTestWhileIdle(boolean testWhileIdle) {
		this.testWhileIdle = testWhileIdle;
	}

	public int getTimeBetweenEvictionRunsMillis() {
		return timeBetweenEvictionRunsMillis;
	}

	public void setTimeBetweenEvictionRunsMillis(int timeBetweenEvictionRunsMillis) {
		this.timeBetweenEvictionRunsMillis = timeBetweenEvictionRunsMillis;
	}
	
	public int getConnectionTimeout() {
		return connectionTimeout;
	}
	
	public void setConnectionTimeout(int connectionTimeout) {
		this.connectionTimeout = connectionTimeout;
	}

	public String toJson() {
		return new Gson().toJson(this);
	}

	public JedisPoolConfig getJedisPoolConfig() {
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxTotal(this.maxTotal);
		config.setMaxIdle(this.maxIdle);
		config.setMinIdle(this.minIdle);
		config.setMaxWaitMillis(this.maxWaitMillis);
		config.setNumTestsPerEvictionRun(this.numTestsPerEvictionRun);
		config.setTestOnBorrow(this.testOnBorrow);
		config.setTestOnReturn(this.testOnReturn);
		config.setTestWhileIdle(this.testWhileIdle);
		config.setTimeBetweenEvictionRunsMillis(this.timeBetweenEvictionRunsMillis);		
		return config;
	}
}
