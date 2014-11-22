package rfx.sample.analytics;

import java.io.File;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import rfx.core.stream.topology.BaseTopology;
import rfx.core.stream.topology.Pipeline;
import rfx.core.util.Utils;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

public class ClickStatsImporterForDMP {
	
	static class ClickLogProcessingTopology extends BaseTopology{
		static final String TOPO_NAME = ClickLogProcessingTopology.class.getSimpleName();
		static final int MAX_POOL_SIZE = 20000;	
		
		public ClickLogProcessingTopology() {
			super(TOPO_NAME);		
		}
		
		@Override
		public BaseTopology buildTopology(){
			return Pipeline.create(MAX_POOL_SIZE, this)				
					.apply(ClickRawLogProcessor.class)		
					.done();
		}	
	}
	
	public static void listFilesForFolder(final File folder, List<String> files) {
		File[] fList = folder.listFiles();
		for (File file : fList) {
			if (file.isFile()) {
				files.add(file.getAbsolutePath());
			} else if (file.isDirectory()) {
				listFilesForFolder(file, files);
			}
		}
	}
	
	public static void createUserLogIndex() throws UnknownHostException{
		MongoClient mongoClient = new MongoClient("localhost", 27017);
		// Now connect to your databases
		DB db = mongoClient.getDB("admin");
		DBCollection tableUserLog = db.getCollection("userlog");		
		//creative unique key indexing
		tableUserLog.createIndex( new BasicDBObject("userid", 1).append("datetime", 1).append("unique", true).append("dropDups", true));
	}

	public static void main(String[] args) throws UnknownHostException {		
		createUserLogIndex();
		
		List<String> files = new ArrayList<String>();
		//FileUtil.listFilesForFolder(new File("/home/trieunt/data/demo_targeting_data/log_click/day=2014-10-24"), files);
		listFilesForFolder(new File("/home/trieu/data/raw_logs/log_click/day=2014-10-25"), files);
		listFilesForFolder(new File("/home/trieu/data/raw_logs/log_click/day=2014-10-26"), files);
		BaseTopology topology = new ClickLogProcessingTopology();
		for (int i = 0; i < files.size(); i++) {
			topology.addDataFile(files.get(i));
		}
		topology.buildTopology().start(800);
		Utils.sleep(10000);
		
		System.out.println("getCurrentRowCount "+ClickRawLogProcessor.getCurrentRowCount());
		
		Utils.foreverLoop();
	}

}
