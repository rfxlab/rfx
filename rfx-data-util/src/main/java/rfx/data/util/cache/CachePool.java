package rfx.data.util.cache;

import java.util.Map;

import com.google.common.cache.LoadingCache;

import rfx.data.util.StringPool;
import rfx.data.util.StringUtil;

/**
 * @author trieu
 * 
 * the cache pool of class DAO implementation
 *
 */
public class CachePool {
	
	LoadingCache<String, Object> cache;
	String keyPrefix;
	Map<String, Long> cachableMethods;
	long defaultExpire = 1;
	 		 
	public CachePool(LoadingCache<String, Object> cache, String keyPrefix, Map<String, Long> cachableMethods, long defaultExpire) {
		super();
		this.cache = cache;
		this.keyPrefix = keyPrefix;
		this.cachableMethods = cachableMethods;
		this.defaultExpire = defaultExpire;
	}
	
	public LoadingCache<String, Object> getCache() {
		return cache;
	}
	public void setCache(LoadingCache<String, Object> cache) {
		this.cache = cache;
	}
	public String getKeyPrefix() {
		return keyPrefix;
	}
	public void setKeyPrefix(String keyPrefix) {
		this.keyPrefix = keyPrefix;
	}
	 
	public String buildKey(String methodName, Object[] args){
		if(StringUtil.isEmpty(keyPrefix)){
			return StringUtil.toString(methodName, StringPool.UNDERLINE, StringUtil.join(args,StringPool.UNDERLINE));
		}
		return StringUtil.toString(keyPrefix, StringPool.UNDERLINE, StringUtil.join(args,StringPool.UNDERLINE));
	}
	
	public long getExpireAfter(String methodName){		
		if(cachableMethods != null){
			long c = cachableMethods.getOrDefault(methodName, 0L);
			return c;
		}		
		return defaultExpire;
	}
	
	@Override
	public String toString() {
		return StringUtil.convertObjectToJson(this);
	}
}
