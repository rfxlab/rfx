package rfx.sample.analytics;

import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import redis.clients.jedis.ShardedJedisPool;
import redis.clients.jedis.exceptions.JedisException;
import rfx.core.configs.ClusterInfoConfigs;
import rfx.core.nosql.jedis.RedisCommand;
import rfx.core.stream.functor.BaseFunctor;
import rfx.core.stream.message.Tuple;
import rfx.core.stream.model.DataFlowInfo;
import rfx.core.stream.topology.BaseTopology;
import rfx.core.util.StringUtil;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class TrueImpRawLogProcessor extends BaseFunctor{
	
	private static final String UNIQUE_VISITOR = "unique-visitor";

	private static final String YYYY_MM_DD_HH = "yyyy-MM-dd HH";


	protected TrueImpRawLogProcessor(DataFlowInfo dataFlowInfo, BaseTopology topology) {
		super(dataFlowInfo, topology);
		
		try {
			if(mongoClient == null){
				mongoClient = new MongoClient("localhost", 27017);
			}			
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	static MongoClient mongoClient;
		
	static AtomicLong genId = new AtomicLong();

	
	public static final long getCurrentRowCount(){
		return genId.get();
	}
	
	void updateStats(BasicDBObject searchQuery, BasicDBObject doc, String bannerId){
		DB db = mongoClient.getDB("admin");
		DBCollection tableUserLog = db.getCollection("userlog");
		db.requestStart();
		try {
		   db.requestEnsureConnection();		   
		   
		  // System.out.println(bannerId);
		   DBCursor cursor = tableUserLog.find(searchQuery);
			if(cursor != null  && cursor.size() > 0) {
				List<DBObject> userLog = cursor.toArray();
				
				BasicDBList viewedads = (BasicDBList) userLog.get(0).get("viewedad");
				if(viewedads == null){
					viewedads = new BasicDBList();
				}				
				viewedads.add(bannerId);
				doc.append("viewedad", viewedads);
				
				int obj = (int) userLog.get(0).get("trueimpcount");
				doc.append("trueimpcount", obj + 1);
				tableUserLog.update(searchQuery, doc);
			} else {
				BasicDBList viewedad = new BasicDBList();
				viewedad.add(bannerId);
				doc.append("viewedad", viewedad);
				
				doc.append("clickadcount", 0)
				.append("trueimpcount", 1)
				.append("pageviewcount", 1);
				
				tableUserLog.insert(doc);
			}

		} finally {
		   db.requestDone();
		}
	}
	
	@Override
	public void onReceive(Object message) throws Exception {
		if (message instanceof Tuple) {			
			Tuple inputTuple = (Tuple) message;
			try {				
				String row = inputTuple.getStringByField("row");
				System.out.println(genId.incrementAndGet() + " # ");
				String[] toks = row.split("\t");
				if(toks.length < 21 ||toks.length > 0){
					inputTuple.clear();
					unhandled(message);
					return;
				}
				
				final String userid = toks[2];
				final String bannerId = toks[9];				
				final String loc = toks[20];
				
				if (userid.contains("=") || StringUtil.isEmpty(bannerId) || StringUtil.isEmpty(userid)) {
					return;
				}
				
				
				SimpleDateFormat formatter = new SimpleDateFormat(YYYY_MM_DD_HH);
				final Date loggedDate = formatter.parse(toks[1]);
				final String hour = formatter.format(loggedDate);
				
				//System.out.println(userid + " "+ formatter.format(date));
//				BasicDBObject doc = new BasicDBObject("userid", userid)
//					.append("genid", 3)
//					.append("loc", loc)
//					.append("datetime", hour )
//					.append("timezone", 7)			
//					.append("adsclusterid", 0);
				//System.out.println(doc.toString());
				ShardedJedisPool jedisPool = ClusterInfoConfigs.load().getClusterInfoRedis().getShardedJedisPool();
				(new RedisCommand<Void>(jedisPool) {
		            @Override
		            public Void build() throws JedisException {
		            	//Pipeline p = jedis.pipelined();
		            	
		            	jedis.pfadd(UNIQUE_VISITOR, userid);
		            	//p.pfadd("u:"+hour, userid);
		            	
//		            	Response<Long> countUV = p.pfcount("unique-visitor");
//		            	Response<Long> countUVPM = p.pfcount("u:"+minuteStr);
		            	//p.sync();
//		            	System.out.println("total unique vistor " + countUV.get());    				            	
//		                return countUVPM.get();
		            	return null;
		            }
			    }).execute();
				//System.out.println("unique vistor per minutes " + count);
				
				//BasicDBObject searchQuery = new BasicDBObject().append("userid", userid).append("datetime", hour);
				//updateStats(searchQuery, doc, bannerId);
				
				//mongoClient.close();
				
			} catch (Exception e) {
				e.printStackTrace();				
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
