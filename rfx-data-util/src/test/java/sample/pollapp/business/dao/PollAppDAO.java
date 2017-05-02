package sample.pollapp.business.dao;

import java.util.List;

import rfx.data.util.cache.Cachable;
import rfx.data.util.cache.CachableMethod;
import rfx.data.util.cache.CacheConfig;
import sample.pollapp.model.Poll;

@CacheConfig( type = CacheConfig.LOCAL_CACHE_ENGINE )
public interface PollAppDAO {
	
	@CachableMethod(expireAfter=300)
	public List<Poll> getAllPolls();
	
	@CachableMethod(expireAfter=300)
	public Poll getPoll(int id);
	
	public boolean save(Poll poll);
}
