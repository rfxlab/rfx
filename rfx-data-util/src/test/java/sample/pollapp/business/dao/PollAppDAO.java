package sample.pollapp.business.dao;

import java.util.List;

import rfx.data.util.cache.Cachable;
import rfx.data.util.cache.CacheConfig;
import sample.pollapp.model.Poll;

@CacheConfig( type = CacheConfig.LOCAL_CACHE_ENGINE, keyPrefix = "poll:", expireAfter = 6 )
public interface PollAppDAO {
	
	@Cachable
	public List<Poll> getAllPolls();
	
	@Cachable
	public Poll getPoll(int id);
	
	public boolean save(Poll poll);
}
