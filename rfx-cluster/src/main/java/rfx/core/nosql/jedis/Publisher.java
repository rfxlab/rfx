package rfx.core.nosql.jedis;

import redis.clients.jedis.Jedis;
import rfx.core.util.StringUtil;

public class Publisher {
    private final Jedis publisherJedis; 
    private final String channel;
 
    public Publisher(Jedis publisherJedis, String channel) {
    	if(publisherJedis == null || channel == null){
    		throw new IllegalArgumentException("publisherJedis == null || channel == null");
    	}
        this.publisherJedis = publisherJedis;
        this.channel = channel;
    }
        
    public void send(String msg) {
    	if(StringUtil.isNotEmpty(msg)){
    		publisherJedis.publish(channel, msg);
    	}
    }    
    
}
