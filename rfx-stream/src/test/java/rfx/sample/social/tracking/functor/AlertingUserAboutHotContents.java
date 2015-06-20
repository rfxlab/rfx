package rfx.sample.social.tracking.functor;

import java.net.URL;
import java.util.Date;
import java.util.List;
import java.util.Map;

import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.ShardedJedisPool;
import redis.clients.jedis.exceptions.JedisException;
import rfx.core.configs.RedisConfigs;
import rfx.core.nosql.jedis.RedisCommand;
import rfx.core.stream.data.RedisRealtimeAnalytics;
import rfx.core.stream.functor.BaseFunctor;
import rfx.core.stream.message.Fields;
import rfx.core.stream.message.Tuple;
import rfx.core.stream.message.Values;
import rfx.core.stream.model.DataFlowInfo;
import rfx.core.stream.topology.BaseTopology;
import rfx.core.stream.util.HashUtil;
import rfx.core.stream.util.ParamUtil;
import rfx.core.stream.util.ua.Client;
import rfx.core.stream.util.ua.Parser;
import rfx.core.util.DateTimeUtil;
import rfx.core.util.LogUtil;
import rfx.core.util.StringUtil;

public class AlertingUserAboutHotContents extends BaseFunctor {
	
	static Parser uaParser = Parser.load();
	static ShardedJedisPool jedisPool = RedisConfigs.load().get("realtimeDataStats").getShardedJedisPool();
	static final int MAX_IP_COUNT_PER_MINUTE = 5;
	static Fields outputFields = new Fields("loggedTime","reading_url","cookie");

	protected AlertingUserAboutHotContents(DataFlowInfo dataFlowInfo, BaseTopology topology) {
		super(dataFlowInfo, topology);
	}

	@Override
	public void onReceive(Object message) throws Exception {
		if (message instanceof Tuple) {			
			Tuple inputTuple = (Tuple) message;
			try {
				String partitionId = inputTuple.getStringByField("partitionId");
				this.counter(StringUtil.toString(this.getMetricKey(), partitionId)).incrementAndGet();
				
				
				final int loggedTime  	= inputTuple.getIntegerByField("loggedtime");
				String query    	= inputTuple.getStringByField("query");
				String userAgent    = inputTuple.getStringByField("useragent");
				String cookie   	= inputTuple.getStringByField("cookie");			
				final String ip       	= inputTuple.getStringByField("ip");
				
				Client client = uaParser.parse(userAgent);	

				if( ! StringUtil.isEmpty(query) ){
					Map<String, List<String>> params = ParamUtil.getQueryMap(query);
					final String reading_url = ParamUtil.getParam(params,"reading_url");	
					final String referrer = ParamUtil.getParam(params,"referrer");
					
					if(reading_url.isEmpty()){
						return;
					}
					System.out.println("reading_url:"+reading_url);
					
					final URL url = new URL(reading_url);
					final String os = client.os.family;
					final String browserType = client.userAgent.family;
					final String browserVersion = client.userAgent.major;
					
					long ipCount = (new RedisCommand<Long>(jedisPool) {
			            @Override
			            public Long build() throws JedisException {
			            	Pipeline p = jedis.pipelined();
			            	long delta = 1L;
			            	Date loggedDate = new Date(loggedTime * 1000L);
			            	String hourStr = DateTimeUtil.getDateHourString(loggedDate);
			            	String minuteStr = DateTimeUtil.formatDateHourMinute(loggedDate);
			            	
			            	p.incrBy("pageview", delta);
			            	p.hincrBy("os", os, delta);
			            	p.hincrBy("browsers", browserType+"-"+browserVersion, delta);
			            	p.hincrBy("host", url.getHost(), delta);
			            	p.hincrBy("ip", ip, delta);			            	
			            		
			            	String hash = HashUtil.hashUrlCrc64(reading_url) + "";
			            	if(! referrer.isEmpty() ){
			            		String hashRef = HashUtil.hashUrlCrc64(referrer) + "";
			            		p.hset("url:"+hashRef, "full-url", referrer);	
			            		p.hincrBy("url:"+hash, "refer-url:"+hashRef, delta);
			            	}			            	
			            	p.hset("url:"+hash, "full-url", reading_url);	
			            	p.hincrBy("t:"+hourStr, "url:"+hash, delta);
			            	Response<Long> countIP = p.hincrBy("t:"+minuteStr, "ip:"+ip, delta);			            	
			            	p.sync();			            	
			            				            	
			                return countIP.get();
			            }
				    }).execute();
					if(ipCount > MAX_IP_COUNT_PER_MINUTE){
						LogUtil.error(new IllegalAccessError("ALERT: IP="+ip + " is flooding our system"));
					} else {
						RedisRealtimeAnalytics.updateStatistics(loggedTime, "pageview");
					}
					Tuple newTuple = new Tuple(outputFields, new Values(loggedTime,reading_url,cookie));
					this.emit(newTuple , self());
					params.clear();
				}
			} catch (IllegalArgumentException e) {
				LogUtil.error(e);
			} catch (Exception e) {
				LogUtil.error(e);
				//log.error(ExceptionUtils.getStackTrace(e));
			} finally {
				inputTuple.clear();
			}		
			this.doPostProcessing();

		} else {
			unhandled(message);
		}
	}

}
