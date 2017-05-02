package rfx.data.util.cache;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

import rfx.data.util.StringPool;

public class GuavaCacheUtil {
	
	final static int CACHE_TIME_OUT = 4;// SECONDS
	final static int CACHE_MAX_SIZE = 100000;
	
	static final Map<String, LoadingCache<String, Object>> cachePool = new HashMap<>();
	
	// local cache for campaign details (Use Java Google Cache)
	static CacheLoader<String, Object> cacheLoader = new CacheLoader<String, Object>() {
		/*
		 * if cache not found, this method will be executed to get from Redis
		 * 
		 * @see com.google.common.cache.CacheLoader#load(java.lang.Object)
		 */
		public Object load(String key) {
			return StringPool.BLANK;
		}
	};
	
	static RemovalListener<String, Object> removalListener = new RemovalListener<String, Object>() {
		public void onRemoval(RemovalNotification<String, Object> removal) {						 
			System.out.println("clear: "+removal.getKey() + " "+removal.getValue());			
		}
	};
	
	public static LoadingCache<String, Object> getLoadingCache(Object object, long maximumSize, long expireAfter){
		String key = String.valueOf(object);
		LoadingCache<String, Object> cache = cachePool.get(key);
		if(cache == null){	
			cache = CacheBuilder
				.newBuilder().maximumSize(maximumSize)
				.expireAfterWrite(expireAfter, TimeUnit.SECONDS)
				//.removalListener(removalListener)
				.build(cacheLoader);
			cachePool.put(key, cache);
		}
		return cache;
	}
	
	 
	
}
