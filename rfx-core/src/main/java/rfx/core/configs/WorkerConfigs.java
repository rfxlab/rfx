package rfx.core.configs;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.google.gson.Gson;

import rfx.core.annotation.AutoInjectedConfig;
import rfx.core.configs.loader.ConfigAutoLoader;
import rfx.core.configs.loader.Configurable;
import rfx.core.configs.loader.ParseConfigHandler;
import rfx.core.configs.loader.ParseXmlObjectHandler;
import rfx.core.model.WorkerInfo;
import rfx.core.util.StringUtil;

public class WorkerConfigs implements Serializable, Configurable {

	private static final long serialVersionUID = -8916185560981048895L;

	@AutoInjectedConfig(injectable = true)
	static WorkerConfigs instance;

	String startWorkerScriptPath = "";
	String debugLogPath = "";
	List<WorkerInfo> allocatedWorkers;
	Map<String, String> customConfigs = new HashMap<>();

	public List<WorkerInfo> getAllocatedWorkers() {
		if (allocatedWorkers == null) {
			allocatedWorkers = new ArrayList<>();
		}
		return allocatedWorkers;
	}

	public void setAllocatedWorkers(List<WorkerInfo> allocatedWorkers) {
		this.allocatedWorkers = allocatedWorkers;
	}

	public String getStartWorkerScriptPath() {
		return startWorkerScriptPath;
	}

	public String getDebugLogPath() {
		return debugLogPath;
	}

	public void setCustomConfig(String name, String value) {
		this.customConfigs.put(name, value);
	}

	public String getCustomConfig(String name) {
		return StringUtil.safeString(customConfigs.get(name));
	}

	public static final WorkerConfigs load() {
		if (instance == null) {
			ConfigAutoLoader.loadAll();
		}
		return instance;
	}

	public String toJson() {
		return new Gson().toJson(this);
	}

	@Override
	public ParseConfigHandler getParseConfigHandler() {
		return new ParseXmlObjectHandler() {
			@Override
			public void injectFieldValue() {
				if(field == null ) {
					return;
				}
				try {
					WorkerConfigs workerConfigs = (WorkerConfigs) configurableObj;
					Element node = xmlNode.select(field.getName()).first();
					if(node == null ) {
						return;
					}
					else {
						System.out.println("WorkerConfigs node: " + node.nodeName());
					}
					if ("list".equals(node.attr("type"))) {
						Elements workerNodes = xmlNode.select(field.getName() + " worker");
						List<WorkerInfo> allocatedWorkers = new ArrayList<>(workerNodes.size());
						for (Element workerNode : workerNodes) {
							int port = StringUtil.safeParseInt(workerNode.select("port").text());
							String mainClass = StringUtil.safeString(workerNode.select("mainClass").text());
							String hostName = StringUtil.safeString(workerNode.select("hostName").text());
							String topology = StringUtil.safeString(workerNode.select("port").attr("topology"));
							allocatedWorkers.add(new WorkerInfo(hostName, port, topology, mainClass));
						}
						workerConfigs.setAllocatedWorkers(allocatedWorkers);
					} else if ("map".equals(node.attr("type"))) {
						Elements configNodes = xmlNode.select(field.getName() + " config");
						for (Element configNode : configNodes) {
							String name = configNode.attr("name");
							String value = configNode.text().trim();
							workerConfigs.setCustomConfig(name, value);
						}
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
