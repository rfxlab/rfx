package rfx.core.stream.connector;

import java.io.File;
import java.util.concurrent.ConcurrentNavigableMap;

import org.mapdb.DB;
import org.mapdb.DBMaker;

import rfx.core.configs.WorkerConfigs;
import rfx.core.util.LogUtil;
import rfx.core.util.StringUtil;
import rfx.core.util.Utils;

public class MapDbConnector {
	
	String topic;
	String workerName = "";
	DB mapDb;
			
	public MapDbConnector(String topic, String workerName) {
		super();
		this.topic = topic;	
		if( StringUtil.isNotEmpty(workerName) ){
			this.workerName = workerName;	
		}		
		initMapDB();
	}
	
	public DB getMapDb() {
		return mapDb;
	}
	
	public String getTopic() {
		return topic;
	}
	
	void initMapDB(){
		String kafkaOffsetPath = StringUtil.toString(WorkerConfigs.load().getKafkaOffsetDbPath(), "/" , topic, "-", workerName);
		if(mapDb == null ){				
			try {
				File file = new File(kafkaOffsetPath);
				File tfile = new File(kafkaOffsetPath+".t");
				if(tfile.isFile()){
					tfile.delete();
				}
				
				boolean shouldMakeNewFile = false;
				if( ! file.exists() ){
					shouldMakeNewFile = file.createNewFile();
					LogUtil.i("MapDbConnector.topic",this.topic+" createNewFile kafka offset at path: "+file.getAbsolutePath() + " : "+shouldMakeNewFile, true);
				} else {
					LogUtil.i("MapDbConnector.topic",this.topic+" loading persistent kafka offset at path: "+file.getAbsolutePath(), true);	
				}
				
				mapDb = DBMaker.newFileDB(file).closeOnJvmShutdown().make();
				
			} catch (Throwable e) {
				if(e instanceof java.io.IOException){
					LogUtil.e("KafkaDataSeeder.MapDbConnector", kafkaOffsetPath + " is NOT valid path");
				} else {
					e.printStackTrace();
					LogUtil.e("KafkaConfigManager", e.toString());
				}
				//WorkerUtil.autoSystemExit(444);
				//FIXME
			} finally {
				if(mapDb == null){
					LogUtil.e("KafkaConfigManager", "kafkaOffsetDb is NULL, failed at mapDb.getTreeMap(\"kafkaOffsetDb\") ");
					//WorkerUtil.autoSystemExit(444);						
				}	
			}
		}
	}

	public synchronized ConcurrentNavigableMap<String,Long>  getOffsetMapDb() {
		//Kafka Offset Storage
		ConcurrentNavigableMap<String,Long> kafkaOffsetDb = mapDb.getTreeMap("kafkaOffsetDb");
		if(kafkaOffsetDb == null){
			LogUtil.e("MapDbConnector.getOffsetMapDb", "mapDb.getTreeMap('kafkaOffsetDb') IS NULL");
			Utils.exitSystemAfterTimeout(500);			
		}
		return kafkaOffsetDb;
	}
	
	public synchronized int getOffsetMapDbSize() {
		//Kafka Offset Storage
		ConcurrentNavigableMap<String,Long> kafkaOffsetDb = mapDb.getTreeMap("kafkaOffsetDb");
		if(kafkaOffsetDb != null){
			return kafkaOffsetDb.size();
		}
		return 0;
	}
	
	public void shutdown(){
	    if(mapDb != null){
		mapDb.commit();
		mapDb.close();
	    }
	}
}