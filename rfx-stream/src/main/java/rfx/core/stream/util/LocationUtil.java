package rfx.core.stream.util;


import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;
import rfx.core.configs.RedisConfigs;
import rfx.core.util.LogUtil;
import rfx.core.util.StringPool;
import rfx.core.util.StringUtil;

import com.google.gson.Gson;

public class LocationUtil {

    private static final String VN = "vn";
	private static final int MAX_CACHE_SIZE = 100000;
	final public static String MAXIP = "4294967295";
    final public static int LOCATION_VIETNAM_UNDEFINED = -101;
    final public static int LOCATION_UNDEFINED = -101;
    final public static int LOCATION_NULL = -101;
    private static Map<Integer, Boolean> provinceCacheMap = new HashMap<>() ; // {24:true,29:true}
    private static Map<String, Boolean> countryCacheMap = new HashMap<>() ; // {VN:true,US:true}
    final static ShardedJedisPool jedisPool;
    static {    	
    	jedisPool = RedisConfigs.load().get("locationData").getShardedJedisPool();
    	provinceCacheMap.put(24, true);
    	provinceCacheMap.put(29, true);
    	countryCacheMap.put(VN, true);
    	countryCacheMap.put("us", true);
    }

    private static NavigableMap<Long, LocationCacheObj> LOCATIONCACHE = new TreeMap<Long, LocationCacheObj>();
    private static NavigableMap<Long, LocationCacheObj> COUNTRYCACHE = new TreeMap<Long, LocationCacheObj>();
    static int cachedTime = 30;
    
	public static Long getHashedNumber(String dottedIP) {
	    String[] addrArray = dottedIP.split("\\.");        
	    long num = 0;        
	    for (int i=0;i<addrArray.length;i++) {            
	        int power = 3-i;            
	        num += ((Integer.parseInt(addrArray[i]) % 256) * Math.pow(256,power));        
	    }        
	    return num;    
	}

    /**
     * Lookup province ip, có cache các dãy IP của HCM, HA NOI (request lớn) ---> improve performance đáng kể
     * @param ipAdress
     * @return
     *   provinceId
     */
    public static LocationCacheObj getVNProvinceFromIp(String ipAdress) {

    	LocationCacheObj locationCacheObj = null;
        long ipLong = 0;
        try {
        	ipLong = getHashedNumber(ipAdress);
            LocationCacheObj floorCacheObj 	  = LOCATIONCACHE.get(LOCATIONCACHE.floorKey(ipLong)); 
            LocationCacheObj ceilCacheObj     = LOCATIONCACHE.get(LOCATIONCACHE.ceilingKey(ipLong));
            if (floorCacheObj.getProvince() != LOCATION_UNDEFINED) {
            	if( floorCacheObj.beginIPNumber == ceilCacheObj.beginIPNumber
            		&& floorCacheObj.endIPNumber == ceilCacheObj.endIPNumber ){            		
            		return floorCacheObj;
            	}
            }
        } catch (Exception ex) {}

        boolean commited = false;
        
        ShardedJedis shardedJedis = null;
        Jedis jedis = null;
        try {        
            shardedJedis = jedisPool.getResource();
            jedis = shardedJedis.getShard(StringPool.BLANK);
            Set<String> floors = jedis.zrevrangeByScore("range_index", String.valueOf(ipLong), "0", 0, 1);
            Set<String> ceils = jedis.zrangeByScore("range_index", String.valueOf(ipLong), MAXIP, 0, 1);
            if (floors.size() == 1 && ceils.size() == 1) {
                String floor = floors.iterator().next();
                String ceil = ceils.iterator().next();
                
                String f0, f1, c0, c1 = "";
                String[] floor_toks = floor.split("-");
				f0 = floor_toks[0];
                f1 = floor_toks[1];
                String[] ceil_toks = ceil.split("-");
				c0 = ceil_toks[0];
                c1 = ceil_toks[1];
                
                if ( (f0.equals(c1) && f1.equals(c0))  || (f0.equals(c0) && f1.equals(c1)) ) {

                	int zone = 0 ;
                	int province =  StringUtil.safeParseInt(jedis.hget("province:" + floor,"province"), LOCATION_UNDEFINED);
                    if( province == LOCATION_UNDEFINED ){
                    	System.out.println( "2.2 endIpNum == ipLong:"+f1+"-"+f0 );
                    	province =  StringUtil.safeParseInt(jedis.hget("province:"+f1+"-"+f0,"province"), LOCATION_UNDEFINED);
                    	zone =  StringUtil.safeParseInt(jedis.hget("province:"+f1+"-"+f0,"zone"), 0);
                    }
                    else{
                    	zone =  StringUtil.safeParseInt(jedis.hget("province:" + floor,"zone"), 0);
                    }

                	long beginIPNumber = Long.parseLong(f0) ;
                	long endIPNumber   = Long.parseLong(f1) ;

                	locationCacheObj = new LocationCacheObj(cachedTime);
                	locationCacheObj.setProvice(province);
                	locationCacheObj.setZone(zone);
                	locationCacheObj.setBeginIPNumber(beginIPNumber);
                	locationCacheObj.setEndIPNumber(endIPNumber);

                    LocationCacheObj ceilCacheObj = new LocationCacheObj();
                    ceilCacheObj.setProvice(province);
                    ceilCacheObj.setZone(zone);
                    ceilCacheObj.setBeginIPNumber(beginIPNumber);
                    ceilCacheObj.setEndIPNumber(endIPNumber);

                    if (LOCATIONCACHE.size() < 100000 ) {
                    	if( provinceCacheMap.get(province) != null ){
	                    	LOCATIONCACHE.put(beginIPNumber, locationCacheObj);
	                		LOCATIONCACHE.put(endIPNumber, ceilCacheObj);
                		}
                	}
                }
            }
            commited = true;
            // zrevrangebyscore range_index 5 0 LIMIT 0 1
        } catch (Exception e) {
            e.printStackTrace();
            LogUtil.e("LocationUtil.getLocationFromIp", e.getMessage());
        } finally {
            if (commited) {
                jedisPool.returnResource(shardedJedis);
            } else {
                jedisPool.returnBrokenResource(shardedJedis);
            }
        }
        if(locationCacheObj == null) {
        	locationCacheObj = new LocationCacheObj();
        }
        return locationCacheObj;
    }

    /**
     * Lookup country ip, có cache các dãy IP của VN, US (request lớn) ---> improve performance đáng kể
     * @param ipAdress
     * @return
     *   countryCode
     */
    public static LocationCacheObj getCountryFromIp(String ipAdress) {

    	LocationCacheObj locationCacheObj = null;
        long ipLong = 0;
        try {
        	ipLong = getHashedNumber(ipAdress);
            LocationCacheObj floorCacheObj 	  = COUNTRYCACHE.get(COUNTRYCACHE.floorKey(ipLong)); 
            LocationCacheObj ceilCacheObj     = COUNTRYCACHE.get(COUNTRYCACHE.ceilingKey(ipLong));
            if (floorCacheObj.getCountryCode() != null) {
            	if( floorCacheObj.beginIPNumber == ceilCacheObj.beginIPNumber
            		&& floorCacheObj.endIPNumber == ceilCacheObj.endIPNumber ){            		
            		return floorCacheObj;
            	}
            }
        } catch (Exception ex) {}

        boolean commited = false;       
        ShardedJedis shardedJedis = null;
        Jedis jedis = null;
        try {
            shardedJedis = jedisPool.getResource();
            jedis = shardedJedis.getShard(StringPool.BLANK);
            Set<String> floors = jedis.zrevrangeByScore("c_range_index", String.valueOf(ipLong), "0", 0, 1);
            Set<String> ceils = jedis.zrangeByScore("c_range_index", String.valueOf(ipLong), MAXIP, 0, 1);
            if (floors.size() == 1 && ceils.size() == 1) {
                String floor = floors.iterator().next();
                String ceil = ceils.iterator().next();

                String f0, f1, c0, c1 = "";
                String[] floor_toks = floor.split("-");
				f0 = floor_toks[0];
                f1 = floor_toks[1];
                String[] ceil_toks = ceil.split("-");
				c0 = ceil_toks[0];
                c1 = ceil_toks[1];

                if ( (f0.equals(c1) && f1.equals(c0)) || (f0.equals(c0) && f1.equals(c1)) ) {

                	String countryCode =  jedis.hget("country:"+floor,"code");
                    if( countryCode==null ){
                    	countryCode =  jedis.hget("country:"+f1+"-"+f0,"code");
                    }

                	long beginIPNumber = Long.parseLong(f0) ;
                	long endIPNumber   = Long.parseLong(f1) ;

                	locationCacheObj = new LocationCacheObj(cachedTime);
                	locationCacheObj.setCountryCode(countryCode);
                	locationCacheObj.setBeginIPNumber(beginIPNumber);
                	locationCacheObj.setEndIPNumber(endIPNumber);

                    LocationCacheObj ceilCacheObj = new LocationCacheObj();
                    locationCacheObj.setCountryCode(countryCode);
                    ceilCacheObj.setBeginIPNumber(beginIPNumber);
                    ceilCacheObj.setEndIPNumber(endIPNumber);

                    if (COUNTRYCACHE.size() < MAX_CACHE_SIZE ) {
                    	if( countryCacheMap.get(countryCode) != null ){
                    		//System.out.println( "COUNTRYCACHE.put "+ beginIPNumber);
                    		COUNTRYCACHE.put(beginIPNumber, locationCacheObj);
                    		COUNTRYCACHE.put(endIPNumber, ceilCacheObj);
                		}
                	}
                }
            }
            commited = true;
            // zrevrangebyscore range_index 5 0 LIMIT 0 1
        } catch (Exception e) {
            e.printStackTrace();
            LogUtil.e("LocationUtil.getLocationFromIp", e.getMessage());
        } finally {
            if (commited) {
                jedisPool.returnResource(shardedJedis);
            } else {
                jedisPool.returnBrokenResource(shardedJedis);
            }
        }
        if(locationCacheObj == null) {
        	locationCacheObj = new LocationCacheObj();
        }
        return locationCacheObj;
    }
    
    public static LocationCacheObj getLocationFromIp(String ip) {        
    	LocationCacheObj locObj = LocationUtil.getVNProvinceFromIp(ip);
		if( locObj.getProvince() == LOCATION_UNDEFINED ){
			LocationCacheObj countryObj = LocationUtil.getCountryFromIp(ip);
			locObj.setCountryCode(countryObj.getCountryCode());
		}
		else{					
			locObj.setCountryCode(VN);
		}
		return locObj;
    }
   
    public static class LocationCacheObj {

    	private long beginIPNumber = 0;
    	private long endIPNumber   = 0;

    	private String countryCode;
        private int province = LOCATION_UNDEFINED;
        private int zone = 0;
        private long cacheDay = new Date().getTime();
        private int liveTime = 30; // minus

        public LocationCacheObj() {
        }

        public LocationCacheObj(int liveTime) {
            this.liveTime = liveTime;
        }

        public int getProvince() {
            return province;
        }

        public void setProvice(int province) {
            this.province = province;
        }

        public int getZone() {
			return zone;
		}

		public void setZone(int zone) {
			this.zone = zone;
		}

		public boolean isExpried() {
            Date currentDate = new Date();
            return ((currentDate.getTime() - cacheDay) / (60 * 1000)) > liveTime;
        }

        public void setLiveTime(int mins) {
            this.liveTime = mins;
        }

		public long getBeginIPNumber() {
			return beginIPNumber;
		}

		public void setBeginIPNumber(long beginIPNumber) {
			this.beginIPNumber = beginIPNumber;
		}

		public long getEndIPNumber() {
			return endIPNumber;
		}

		public void setEndIPNumber(long endIPNumber) {
			this.endIPNumber = endIPNumber;
		}

		public String getCountryCode() {
			return countryCode;
		}

		public void setCountryCode(String countryCode) {
			this.countryCode = countryCode;
		}

		@Override
		public String toString() {
			return new Gson().toJson(this);
		}
    }
    
    public static void main(String[] args) {
    	String ip = "172.30.113.160";
		LocationCacheObj locObj = LocationUtil.getVNProvinceFromIp(ip );
		int locProvince = locObj.getProvince();
		int locZone = locObj.getZone();
		if( locProvince == LocationUtil.LOCATION_UNDEFINED ){
			LocationCacheObj countryObj = LocationUtil.getCountryFromIp(ip);
			locObj.setCountryCode(countryObj.getCountryCode());
			locProvince = 9999;
		}
		else{
			locObj.setCountryCode("vn");
		}
		
		String locCountry = locObj.getCountryCode(); 
		String location = StringUtil.join("-",locProvince, locZone, locCountry);
		System.out.println(location);
	}

}
