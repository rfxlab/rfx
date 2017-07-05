package rfx.sample.social.tracking.functor;

import java.util.Date;

import redis.clients.jedis.Pipeline;
import redis.clients.jedis.ShardedJedisPool;
import redis.clients.jedis.exceptions.JedisException;
import rfx.core.configs.RedisConfigs;
import rfx.core.nosql.jedis.RedisCommand;
import rfx.core.stream.functor.BaseFunctor;
import rfx.core.stream.message.Tuple;
import rfx.core.stream.model.DataFlowInfo;
import rfx.core.stream.topology.BaseTopology;
import rfx.core.util.DateTimeUtil;
import rfx.core.util.HashUtil;

/**
 * @author trieu 
 * 
 * find like stats from URL on Facebook
 *
 */
public class FindingSocialTrends extends BaseFunctor {

	static ShardedJedisPool jedisPool = RedisConfigs.load().get("realtimeDataStats").getShardedJedisPool();
	
	protected FindingSocialTrends(DataFlowInfo dataFlowInfo, BaseTopology topology) {
		super(dataFlowInfo, topology);
	}
	
	@Override
	public void onReceive(Object message) throws Exception {
		if (message instanceof Tuple) {			
			Tuple tuple = (Tuple) message;
			final int loggedTime = tuple.getIntegerByField("loggedTime");
			final String reading_url = tuple.getStringByField("reading_url");			        	
			Date loggedDate = new Date(loggedTime * 1000L);			
			
			final String minuteStr = DateTimeUtil.formatDateHourMinute(loggedDate);			 						
			int fbLike = (new RedisCommand<Integer>(jedisPool) {
	            @Override
	            public Integer build() throws JedisException {
	            	Pipeline p = jedis.pipelined();
	            
	            	//get Facebook like count from Facebook API
	            	int fbLike = 0;//FIXME SocialAnalyticsUtil.getFacebookLikeCount(reading_url);	
	            	long hash = HashUtil.hashUrlCrc64(reading_url);
	            	
	            	p.hset("url:"+hash, "facebook-like", fbLike+"");	    			
	    			p.zadd("trending-urls:"+minuteStr, fbLike, hash+"");
	    			
	        		p.sync();
	                return fbLike;
	            }
		    }).execute();
			System.out.println("###reading_url: "+reading_url+" like "+fbLike);
			
		}
	}

}
//TODO https://github.com/addthis/stream-lib