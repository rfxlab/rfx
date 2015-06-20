package rfx.core.stream.emitters;

import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.ShardedJedisPool;
import redis.clients.jedis.exceptions.JedisException;
import rfx.core.configs.RedisConfigs;
import rfx.core.nosql.jedis.RedisCommand;
import rfx.core.nosql.jedis.Subscriber;
import rfx.core.stream.model.DataFlowInfo;

public abstract class RedisDataEmitter implements Runnable {	
	static Logger logger = LoggerFactory.getLogger(RedisDataEmitter.class);	 
	private static AtomicLong count = new AtomicLong();
	
	protected DataFlowInfo routingFlow;
	protected String channelName;
	
	public RedisDataEmitter(DataFlowInfo routingFlow, String channelName) {
		super();
		this.routingFlow = routingFlow;
		this.channelName = channelName;
	}

	@Override
	public void run() {	
		//FIXME
    	ShardedJedisPool jedisPool = RedisConfigs.load().get("clusterInfoRedis").getShardedJedisPool();
		(new RedisCommand<Void>(jedisPool) {				
			@Override
			public Void build() throws JedisException {
				try {
					 Subscriber subscriber = new Subscriber(){
						@Override
						public void onMessage(String channel, String message) {
							long c = count.incrementAndGet();
							logger.info(c+" Message received. Channel: {}, Msg: {}", channel, message);
							sendTupleToSubscribers(message);									
						}
					 };
				     logger.info("Subscribing to \""+channelName+"\". This thread will be blocked.");
				     jedis.subscribe(subscriber, channelName);
				} catch (Exception e) {
					e.printStackTrace();
				}
				return null;
			}
		}).execute();
	}
	
	public abstract void sendTupleToSubscribers(String message);
}
