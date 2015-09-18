package rfx.data.util.cache;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;

public abstract class NoSqlCacheLoading implements LoadingCache<String, Object>{

	@Override
	public void invalidateAll(Iterable<?> arg0) {
		
	}

	@Override
	public void putAll(Map<? extends String, ? extends Object> arg0) {
		
	}

	@Override
	public CacheStats stats() {
		return null;
	}

	@Override
	public Object apply(String arg0) {		
		return null;
	}

	@Override
	public ConcurrentMap<String, Object> asMap() {		
		return null;
	}



	@Override
	public ImmutableMap<String, Object> getAll(Iterable<? extends String> arg0)
			throws ExecutionException {	
		return null;
	}

	@Override
	public Object getUnchecked(String arg0) {
		return null;
	}

	@Override
	public void refresh(String arg0) {
	}
	
	@Override
	public void cleanUp() {
	}

	@Override
	public Object get(String arg0, Callable<? extends Object> arg1)
			throws ExecutionException {
		return null;
	}

	@Override
	public ImmutableMap<String, Object> getAllPresent(Iterable<?> arg0) {		
		return null;
	}
	
	@Override
	public void invalidate(Object arg0) {
		
	}
	
	@Override
	public Object getIfPresent(Object arg0) {
		return null;
	}
}
