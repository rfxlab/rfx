package rfx.sample.social.tracking.functor;

import java.util.Date;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import redis.clients.jedis.Pipeline;
import redis.clients.jedis.ShardedJedisPool;
import redis.clients.jedis.exceptions.JedisException;
import rfx.core.configs.ClusterInfoConfigs;
import rfx.core.nosql.jedis.RedisCommand;
import rfx.core.stream.functor.BaseFunctor;
import rfx.core.stream.message.Tuple;
import rfx.core.stream.model.DataFlowInfo;
import rfx.core.stream.topology.BaseTopology;
import rfx.core.stream.util.HashUtil;
import rfx.core.stream.util.SocialAnalyticsUtil;
import rfx.core.util.DateTimeUtil;
import rfx.core.util.HttpClientUtil;

/**
 * @author trieu (Open Source and Implemented at mc2ads Reactive Big Data Lab)
 * 
 * the class functor, show you how real-time computation is implemented 
 * to find trending keywords and most liked URL on Facebook
 *
 */
public class FindingSocialTrends extends BaseFunctor {

	static ShardedJedisPool jedisPool = ClusterInfoConfigs.load().getClusterInfoRedis().getShardedJedisPool();
	
	protected FindingSocialTrends(DataFlowInfo dataFlowInfo, BaseTopology topology) {
		super(dataFlowInfo, topology);
	}
	
	@Override
	public void onReceive(Object message) throws Exception {
		if (message instanceof Tuple) {			
			Tuple tuple = (Tuple) message;
			final int loggedTime = tuple.getIntegerByField("loggedTime");
			final String reading_url = tuple.getStringByField("reading_url");
			String cookie = tuple.getStringByField("cookie");        	
			Date loggedDate = new Date(loggedTime * 1000L);
			final Document doc = Jsoup.parse(HttpClientUtil.executeGet(reading_url));
			final Elements nodes = doc.select("meta[name=keywords]");
			final String minuteStr = DateTimeUtil.formatDateHourMinute(loggedDate);
			 						
			int fbLike = (new RedisCommand<Integer>(jedisPool) {
	            @Override
	            public Integer build() throws JedisException {
	            	Pipeline p = jedis.pipelined();
	            
	            	//get Facebook like count from Facebook API
	            	int fbLike = SocialAnalyticsUtil.getFacebookLikeCount(reading_url);	
	            	String hash = HashUtil.hashUrlCrc64(reading_url) + "";
	            	
	            	p.hset("url:"+hash, "facebook-like", fbLike+"");	    			
	    			p.zadd("trending-urls:"+minuteStr, fbLike, hash);
	    			
	            	if(nodes.size()>0){
	            		String keywords = nodes.get(0).attr("content");	 
	            		if( ! keywords.isEmpty() ){
	            			p.hset("url:"+hash, "keywords", keywords);			    			
			    			System.out.println("keywords:"+keywords);
			    			String[] words = keywords.split(",");
			    			for (String word : words) {
			    				p.zincrby("trending-keywords:"+minuteStr, 1, word);
							}
	            		}		    			
	    			}	    			
	    			p.sync();
	                return fbLike;
	            }
		    }).execute();
			System.out.println("###reading_url: "+reading_url+" like "+fbLike);
			
		}
	}

}
//TODO https://github.com/addthis/stream-lib