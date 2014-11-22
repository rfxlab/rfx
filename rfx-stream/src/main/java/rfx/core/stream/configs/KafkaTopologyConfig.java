package rfx.core.stream.configs;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import rfx.core.configs.loader.Configurable;
import rfx.core.configs.loader.ParseConfigHandler;
import rfx.core.configs.loader.ParseXmlListObjectHandler;
import rfx.core.stream.util.ListUtil;
import rfx.core.util.StringUtil;

import com.google.gson.Gson;


public class KafkaTopologyConfig implements Serializable, Configurable{

	private static final long serialVersionUID = -1816966153179718266L;
	
	private static Map<String,KafkaTopologyConfig> kafkaTopologyConfigs = new HashMap<String, KafkaTopologyConfig>();
	
	String name;	
	String brokerList;
	String topology;	
	boolean autoStart = true;
		
	public boolean isAutoStart() {
		return autoStart;
	}

	public void setAutoStart(boolean autoStart) {
		this.autoStart = autoStart;
	}

	public String getName() {
		return name;
	}

	public String getBrokerList() {
		return brokerList;
	}

	public String getTopology() {
		return topology;
	}
	

	public void setName(String name) {
		this.name = name;
	}

	public void setBrokerList(String brokerList) {
		this.brokerList = brokerList;
	}

	public void setTopology(String topology) {
		this.topology = topology;
	}

	
	public static KafkaTopologyConfig getKafkaConfigsForTopic(String topic) {
		if(kafkaTopologyConfigs.containsKey(topic)){
			return kafkaTopologyConfigs.get(topic);	
		}
		return null;
	}
	
	public static List<String> getBrokerList(String topic) {
		if(kafkaTopologyConfigs.containsKey(topic)){
			String s = StringUtil.safeString( kafkaTopologyConfigs.get(topic).getBrokerList());
			String[] toks = s.split(",");
			return ListUtil.toList(toks);
		}
		return new ArrayList<>(0);
	}
	
	public static List<String> getKafkaTopic() {	    
	    List<String> result = new ArrayList<String>(kafkaTopologyConfigs.entrySet().size());
	    for (Map.Entry<String,KafkaTopologyConfig> entry : kafkaTopologyConfigs.entrySet()) {
	    	result.add(entry.getKey());
	    }
	    return result;
	}	
	
	public static String getTopologyClassPath(String topic) {
		System.out.println(kafkaTopologyConfigs);
		if(kafkaTopologyConfigs.get(topic) != null){			
			String topologyClassPath = kafkaTopologyConfigs.get(topic).getTopology();
			System.out.println(topic +" => "+topologyClassPath);
			return topologyClassPath;
		}
		System.err.println("topic:" + topic + " is NOT found in configs");		
		return null;
	}

	public String toJson(){
		return new Gson().toJson(this);
	}

	@Override
	public ParseConfigHandler getParseConfigHandler() {	
		return new ParseXmlListObjectHandler() {		
			@Override
			public void injectFieldValue() {
				KafkaTopologyConfig kafkaConfig = (KafkaTopologyConfig)configurableObj;

				String name = xmlNode.attr("name");
				String topology = xmlNode.attr("topology");
				
				boolean autoStart = true;
				if(StringUtil.isNotEmpty(xmlNode.attr("autoStart"))){
					autoStart = "true".equalsIgnoreCase(xmlNode.attr("autoStart"));
				}
				
				if(topology != null && name != null){
					kafkaConfig.setName(name);
					kafkaConfig.setTopology(topology);
					kafkaConfig.setAutoStart(autoStart);
					kafkaConfig.setBrokerList(xmlNode.select("brokers").text());					
					kafkaTopologyConfigs.put(name, kafkaConfig);
				}
				
			}
		};
	}
	
	@Override
	public String toString() {		
		return new Gson().toJson(this);
	}
	
}
