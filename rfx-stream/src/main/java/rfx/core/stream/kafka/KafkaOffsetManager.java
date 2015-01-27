package rfx.core.stream.kafka;

import java.io.File;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import rfx.core.configs.WorkerConfigs;
import rfx.core.util.FileUtils;
import rfx.core.util.StringUtil;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class KafkaOffsetManager {
	
	String topic;
	String workerName = "";
	ConcurrentMap<String, Long> offsetMap;
	String kafkaOffsetPath;
			
	public KafkaOffsetManager(String topic, String workerName) {
		super();
		this.topic = topic;	
		if( StringUtil.isNotEmpty(workerName) ){
			this.workerName = workerName;	
		}		
		init();
	}
	
	public ConcurrentMap<String, Long> getOffsetMap() {
		return offsetMap;
	}
	
	public String getTopic() {
		return topic;
	}
	
	private void init(){
		kafkaOffsetPath = StringUtil.toString(WorkerConfigs.load().getKafkaOffsetDbPath(), "/" , topic, "-", workerName);		
		offsetMap = new ConcurrentHashMap<>();
		File file = new File(kafkaOffsetPath);
		if(file.isFile()){
			try {
				Type type = new TypeToken<Map<String, Long>>(){}.getType();
				String json = FileUtils.readFileAsString(kafkaOffsetPath);
				if(StringUtil.isNotEmpty(json)){
					Map<String, Long> map = new Gson().fromJson(json, type);
					if(map != null){
						offsetMap.putAll(map);
					}
				}
			} catch (Throwable e) {				
				e.printStackTrace();
			}	
		}
		else {
			try {
				file.createNewFile();
			} catch (Throwable e) {
				e.printStackTrace();
			}
		}		
	}

	public void setData(String key, long value){
		offsetMap.put(key, value);
	}
	
	public long getData(String key){
		return StringUtil.safeParseLong(offsetMap.get(key),-1L);
	}
	
	public int getOffsetMapSize() {		
		return offsetMap.size();
	}
	
	public void save(){	    
	    FileUtils.writeStringToFile(kafkaOffsetPath, new Gson().toJson(offsetMap));
	}
	
	public void asynchSave(){	    
		//TODO
	}
	
	public void shutdown(){
		//TODO
		save();
	}
}