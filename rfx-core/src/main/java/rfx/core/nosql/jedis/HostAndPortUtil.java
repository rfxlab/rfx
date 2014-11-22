package rfx.core.nosql.jedis;
import java.util.ArrayList;
import java.util.List;

public class HostAndPortUtil {
    private static List<RedisInfo> redisHostAndPortList = new ArrayList<RedisInfo>();
    

    static {
      
    }

    public static List<RedisInfo> parseHosts() {
    	return redisHostAndPortList;
    }
    
    public static List<RedisInfo> getRedisServers() {
        return redisHostAndPortList;
    }
    


}