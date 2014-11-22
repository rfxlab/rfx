package rfx.sample.social.tracking.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import redis.clients.jedis.ShardedJedisPool;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.exceptions.JedisException;
import rfx.core.configs.ClusterInfoConfigs;
import rfx.core.nosql.jedis.RedisCommand;

public class TrackingReportUtil {
	static ShardedJedisPool jedisPool = ClusterInfoConfigs.load().getClusterInfoRedis().getShardedJedisPool();

	public static List<String> getTrendingKeywords(final String minuteStr){
		List<String> trendingKeywords = (new RedisCommand<List<String>>(jedisPool) {
            @Override
            public List<String> build() throws JedisException {	            	
            	Set<Tuple> set = jedis.zrevrangeWithScores("trending-keywords:"+minuteStr, 0, -1);
            	List<String> list = new ArrayList<String>(10);
            	for (Tuple e : set) {
            		String str = e.getElement() + ":" + e.getScore();
            		list.add(str);
            		if(list.size()>10){
            			break;
            		}
				}
                return list;
            }
	    }).execute();	
		return trendingKeywords;
	}
	
	public static List<String> getTrendingUrls(final String minuteStr){
		List<String> trendingUrls = (new RedisCommand<List<String>>(jedisPool) {
            @Override
            public List<String> build() throws JedisException {				            	
            	Set<Tuple> set = jedis.zrevrangeWithScores("trending-urls:"+minuteStr, 0, -1);
            	List<String> list = new ArrayList<String>(10);
            	for (Tuple e : set) {
            		String url = jedis.hget("url:"+ e.getElement(), "full-url");
            		String str = url+ ":" + e.getScore();
            		list.add(str);
            		if(list.size()>10){
            			break;
            		}
				}
                return list;
            }
	    }).execute();
		return trendingUrls;
	}
}
