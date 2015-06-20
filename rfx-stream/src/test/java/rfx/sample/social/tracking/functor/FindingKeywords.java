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
import rfx.core.util.DateTimeUtil;
import rfx.core.util.HttpClientUtil;

/**
 * @author trieu 
 * 
 * to find keywords 
 *
 */
public class FindingKeywords extends BaseFunctor {

	static ShardedJedisPool jedisPool = ClusterInfoConfigs.load().getClusterInfoRedis().getShardedJedisPool();
	
	protected FindingKeywords(DataFlowInfo dataFlowInfo, BaseTopology topology) {
		super(dataFlowInfo, topology);
	}
	
	@Override
	public void onReceive(Object message) throws Exception {
		if (message instanceof Tuple) {			
			Tuple tuple = (Tuple) message;
			final int loggedTime = tuple.getIntegerByField("loggedTime");
			final String reading_url = tuple.getStringByField("reading_url");
			        	
			Date loggedDate = new Date(loggedTime * 1000L);
			final Document doc = Jsoup.parse(HttpClientUtil.executeGet(reading_url));
			final Elements nodes = doc.select("meta[name=keywords]");
			final String minuteStr = DateTimeUtil.formatDateHourMinute(loggedDate);
			 						
			(new RedisCommand<Void>(jedisPool) {
	            @Override
	            public Void build() throws JedisException {
	            	Pipeline p = jedis.pipelined();
	            	String hash = HashUtil.hashUrlCrc64(reading_url) + "";
	            	
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
	                return null;
	            }
		    }).execute();			
			
		}
	}

}
//TODO https://github.com/addthis/stream-lib