package rfx.core.nosql.jedis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;
import redis.clients.jedis.exceptions.JedisException;
import rfx.core.util.LogUtil;
import rfx.core.util.StringPool;

public abstract class RedisCommand<T> {
	protected ShardedJedisPool jedisPool;
	protected ShardedJedis shardedJedis = null;
	protected Jedis jedis = null;

	public RedisCommand(ShardedJedisPool jedisPool) {
		super();
		if (jedisPool == null) {
			throw new IllegalArgumentException("jedisPool is NULL!");
		}
		this.jedisPool = jedisPool;		
	}

	public T execute() {
		boolean commited = false;
		T rs = null;
		try {
			shardedJedis = jedisPool.getResource();
			if (shardedJedis != null) {				
				jedis = shardedJedis.getShard(StringPool.BLANK);
				rs = build();
				commited = true;
			}
		} catch (Exception e) {			
			LogUtil.e("JedisPool: "+jedisPool.toString(), e.toString());
		} finally {			
			freeRedisResource(jedisPool, shardedJedis, commited);
		}
		return rs;
	}
	
	public static void freeRedisResource(ShardedJedisPool jedisPool, ShardedJedis shardedJedis, boolean isCommited){
		if (shardedJedis != null && jedisPool != null) {
			if (isCommited) {
				jedisPool.returnResource(shardedJedis);
			} else {
				jedisPool.returnBrokenResource(shardedJedis);
			}
		}
	}
	
	//define the logic at implementer
	protected abstract T build() throws JedisException;
}