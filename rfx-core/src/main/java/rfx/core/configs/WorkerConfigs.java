package rfx.core.configs;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import rfx.core.annotation.AutoInjectedConfig;
import rfx.core.configs.loader.ConfigAutoLoader;
import rfx.core.configs.loader.Configurable;
import rfx.core.configs.loader.ParseConfigHandler;
import rfx.core.configs.loader.ParseXmlObjectHandler;
import rfx.core.model.WorkerInfo;
import rfx.core.util.StringUtil;

import com.google.gson.Gson;

public class WorkerConfigs implements Serializable, Configurable{

	private static final long serialVersionUID = -8916185560981048895L;
	
	@AutoInjectedConfig(injectable = true)
	static WorkerConfigs instance;
	
	String prefixWorkerName;
	String hostName;
	List<WorkerInfo> allocatedWorkers;
	String allocatedCmdActorPortRanges;
	String startWorkerScriptPath;
	String debugLogPath;
	String kafkaOffsetDbPath;
	String mainClass;
	
	
    public String getMainClass() {
        return mainClass;
    }

    public List<WorkerInfo> getAllocatedWorkers() {
		if(allocatedWorkers == null){
			allocatedWorkers = new ArrayList<>();
		}
		return allocatedWorkers;
	}

	public void setAllocatedWorkers(List<WorkerInfo> allocatedWorkers) {
		this.allocatedWorkers = allocatedWorkers;
	}
	
	public String getHostName() {
		return hostName;
	}
	
	public String getPrefixWorkerName() {
		return prefixWorkerName;
	}

	public String getAllocatedCmdActorPortRanges() {
		return allocatedCmdActorPortRanges;
	}

	public String getStartWorkerScriptPath() {
		return startWorkerScriptPath;
	}
	
	public String getDebugLogPath() {
		return debugLogPath;
	}
	
	public String getKafkaOffsetDbPath() {
		return kafkaOffsetDbPath;
	}

	public static final WorkerConfigs load() {	
		if(instance == null){
			ConfigAutoLoader.loadAll();
		}
		return instance;
	}
	
	public String toJson(){
		return new Gson().toJson(this);
	}

	@Override
	public ParseConfigHandler getParseConfigHandler() {		
		return new ParseXmlObjectHandler() {			
			@Override
			public void injectFieldValue() {
				try {
					WorkerConfigs workerConfigs = (WorkerConfigs)configurableObj;
					
					Element node = xmlNode.select(field.getName()).first();
					if( "list".equals(node.attr("type") )){
						Elements workerNodes =  xmlNode.select(field.getName()+" worker");
						List<WorkerInfo> allocatedWorkers = new ArrayList<>(workerNodes.size());
						for (Element workerNode : workerNodes) {
							int port = StringUtil.safeParseInt(workerNode.select("port").text());
							String mainClass = StringUtil.safeString(workerNode.select("mainClass").text());
							String topology = StringUtil.safeString(workerNode.select("port").attr("topology"));
							allocatedWorkers.add(new WorkerInfo(workerConfigs.hostName, port, topology, mainClass));
						}
						workerConfigs.setAllocatedWorkers(allocatedWorkers);
					} else {
						field.set(configurableObj, node.text());
					}
				} catch (Exception e) {
					e.printStackTrace();
				}				
			}
		};
	}
}
