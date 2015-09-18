package rfx.data.util.cache;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface CacheConfig {
	String keyPrefix() default ""; //the prefix of key
	long maximumSize() default -1; //unlimited
	long expireAfter() default 3600; //1 hous
	int type() default LOCAL_CACHE_ENGINE; //default using Google Guava Cache and store items in local JVM Memory

	boolean allMethods = true; // cache all methods in DAOImpl class
	String poolName() default ""; // for Memcache or Redis or orther Key-Value NoSQL database
	
	public static int LOCAL_CACHE_ENGINE = 1;
	public static int MEMCACHE_CACHE_ENGINE = 2;
	public static int REDIS_CACHE_ENGINE = 3;
}