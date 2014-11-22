package rfx.sample.analytics;

import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import rfx.core.stream.functor.BaseFunctor;
import rfx.core.stream.message.Tuple;
import rfx.core.stream.model.DataFlowInfo;
import rfx.core.stream.topology.BaseTopology;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class ClickRawLogProcessor extends BaseFunctor{
	
	protected ClickRawLogProcessor(DataFlowInfo dataFlowInfo, BaseTopology topology) {
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
	
	void updateStats(BasicDBObject searchQuery, BasicDBObject doc, String[] toks){
		DB db = mongoClient.getDB("admin");
		DBCollection tableUserLog = db.getCollection("userlog");
		db.requestStart();
		try {
		   db.requestEnsureConnection();
		   
		   DBCursor cursor = tableUserLog.find(searchQuery);
			if(cursor != null  && cursor.size() > 0) {
				List<DBObject> userLog = cursor.toArray();
				BasicDBList refererads = (BasicDBList) userLog.get(0).get("refererad");
				refererads.add(toks[9]);
				doc.append("refererad", refererads);
				
				int obj = (int) userLog.get(0).get("clickadcount");
				doc.append("clickadcount", obj + 1);
				tableUserLog.update(searchQuery, doc);
			} else {
				BasicDBList refererads = new BasicDBList();
				refererads.add(toks[9]);
				doc.append("refererad", refererads);
				doc.append("clickadcount", 1)
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
				if (toks[2].contains("=")) {
					return;
				}
				
				SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH");
				Date date = formatter.parse(toks[1]);
				
				BasicDBObject doc = new BasicDBObject("userid", toks[2])
					.append("genid", toks[20])
					.append("loc", "")
					.append("datetime", formatter.format(date))
					.append("timezone", 7)
					.append("adsclusterid", 0);
				
				BasicDBObject searchQuery = new BasicDBObject().append("userid", toks[2]).append("datetime", formatter.format(date));
				updateStats(searchQuery, doc, toks);
				
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
