package rfx.core.nosql.jedis;

/**
 * @author trieu
 * 
 * any object must implements this interface for useing autosharding redis
 *
 */
public interface Shardable {
	public Number getShardKey();
}
