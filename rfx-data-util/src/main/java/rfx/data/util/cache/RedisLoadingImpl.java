package rfx.data.util.cache;

import java.util.concurrent.ExecutionException;

import rfx.data.util.StringUtil;

public class RedisLoadingImpl extends NoSqlCacheLoading{
	String poolname;
	
	public RedisLoadingImpl(String poolname) {
		if(StringUtil.isEmpty(poolname)){
			throw new IllegalArgumentException("Redis poolname can NOT Empty");
		}
		this.poolname = poolname;
	}
	
	public boolean flush(){
		boolean rs = false;
		try {
			//TODO
		} catch (Exception e) {
			e.printStackTrace();
		}
		return rs;
	}
	
	@Override
	public void invalidateAll() {
		flush();
	}
	
	@Override
	public Object get(String key) throws ExecutionException {
		String json = null;
		try {
			//TODO
		} catch (Exception e) {
			e.printStackTrace();
		}
		return json;
	}
	
	@Override
	public void put(String key, Object obj) {
		try {
			//TODO
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public long size() {		
		try {
			//TODO
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}
}
