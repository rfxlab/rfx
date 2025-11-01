package rfx.core.configs.loader;

import rfx.core.configs.RedisConfigs;


public class ConfigAutoLoader {
	

	static boolean loadAll = false;
	
	public synchronized final static void loadAll(){
		if(loadAll){
			return;
		}
		
	    RedisConfigs.load();
	    
		loadAll = true;
	}
}
