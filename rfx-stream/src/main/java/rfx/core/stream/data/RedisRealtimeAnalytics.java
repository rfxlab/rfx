package rfx.core.stream.data;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.ShardedJedisPool;
import redis.clients.jedis.exceptions.JedisException;
import rfx.core.configs.RedisConfigs;
import rfx.core.nosql.jedis.RedisCommand;
import rfx.core.stream.data.metric.MetricStatisticsData;
import rfx.core.util.DateTimeUtil;
import rfx.core.util.StringUtil;


public class RedisRealtimeAnalytics {
	
	public static final String MONITOR_SUMMARY = "summary";
	public static final String DATE_FORMAT_PATTERN = "yyyy-MM-dd";
	public static final String DATE_HOUR_FORMAT_PATTERN = "yyyy-MM-dd-HH";
	public static final String DATE_HOUR_MINUTE_FORMAT_PATTERN = "yyyy-MM-dd-HH-mm";
	public static final String DATE_HOUR_MINUTE_SECOND_FORMAT_PATTERN = "yyyy-MM-dd-HH-mm-ss";
		
	static ShardedJedisPool poolRealtimeDataStats = RedisConfigs.load().get("realtimeDataStats").getShardedJedisPool();
	
	static final int EXPIRED_2_DAYS = 172800;
	static final int EXPIRED_8_DAYS = EXPIRED_2_DAYS * 4;	
	static final int EXPIRED_3_HOURS = 10800;
	static final int EXPIRED_10_MINUTES = 600;	
	
	private static boolean update(int loggedTime, final Iterator<String> metricKeysIterator ){
		Date date = new Date(loggedTime * 1000L);
		String dateStr = DateTimeUtil.formatDate(date, DATE_FORMAT_PATTERN);
		String datehourStr = DateTimeUtil.formatDate(date, DATE_HOUR_FORMAT_PATTERN);		
		String minuteStr = DateTimeUtil.formatDate(date ,DATE_HOUR_MINUTE_FORMAT_PATTERN);
		String secondStr = DateTimeUtil.formatDate(date ,DATE_HOUR_MINUTE_SECOND_FORMAT_PATTERN);
		
		final String dKey = "t:" + dateStr;
		final String dhKey = "t:" + datehourStr;
		final String dhmKey = "t:"+minuteStr;
		final String dhmsKey = "t:"+secondStr;
		
		boolean rs = (new RedisCommand<Boolean>(poolRealtimeDataStats){
			@Override
			protected Boolean build() throws JedisException {
				Pipeline pipe = jedis.pipelined();
				// statistics for time-series (sum by grouping to hourly and daily)
				while (metricKeysIterator.hasNext()) {
					String metricKey = (String) metricKeysIterator.next();
					//summary monitor
					pipe.hincrBy("summary", metricKey, 1L);

					//day monitor
					pipe.hincrBy(dKey, metricKey, 1L);
					
					//hour monitor
					pipe.hincrBy(dhKey, metricKey, 1L);
					pipe.expire(dhmKey, EXPIRED_8_DAYS);
					
					//minute monitor
					pipe.hincrBy(dhmKey, metricKey, 1L);
					pipe.expire(dhmKey, EXPIRED_3_HOURS);
					
					//second monitor
					pipe.hincrBy(dhmsKey, metricKey, 1L);
					pipe.expire(dhmsKey, EXPIRED_10_MINUTES);
				}		
				pipe.sync();				
				return true;
			}
		}).execute();
		return rs;
	}
	
	public static boolean updateStatistics(int loggedTime,final String ... metricKeys){
		return update(loggedTime, Arrays.asList(metricKeys).iterator());
	}
	
	public static boolean updateStatistics(int loggedTime,Set<String> metricKeys){
		return update(loggedTime, metricKeys.iterator());
	}
	
	public static MetricStatisticsData getMetricStatisticsData(Date currDate, Jedis jedis, String metric){
		Calendar calendar = Calendar.getInstance();
		final SimpleDateFormat formatterMinute = new SimpleDateFormat("yyyy-MM-dd-HH-mm");
		final SimpleDateFormat formatterSecond = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
		
		//minute
		Map<Integer, Integer> statsPerMinute = new LinkedHashMap<>(60);
		for (int i = 60; i >= 1; i--) {
			int v = (-1)*(60 - i);				
			calendar.setTime(currDate);
			calendar.add(Calendar.MINUTE, v); 				
			String mkey = "t:" + formatterMinute.format(calendar.getTime());
			System.out.println(mkey + " " + metric);
			int imp = StringUtil.safeParseInt(jedis.hget(mkey, metric));						
			statsPerMinute.put(v, imp);
		}
		
		//second
		calendar.setTime(currDate);
		calendar.add(Calendar.SECOND, -100 );
		Date beginGetSecond = calendar.getTime();
		Map<Integer, Integer> statsPerSecond = new LinkedHashMap<>(60);
		for (int i = 59; i >= 0; i--) {
			int v = (-1)*(60 - i);				
			calendar.setTime(beginGetSecond);
			calendar.add(Calendar.SECOND, v); 				
			String skey = "t:" + formatterSecond.format(calendar.getTime());
			System.out.println(skey + " " + metric);
			int imp = StringUtil.safeParseInt(jedis.hget(skey, metric));						
			statsPerSecond.put(v, imp);
		}		
		return new MetricStatisticsData(metric, statsPerMinute, statsPerSecond);
	}
	
}

