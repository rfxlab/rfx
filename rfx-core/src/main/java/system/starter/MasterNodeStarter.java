package system.starter;

import rfx.core.configs.ClusterInfoConfigs;
import rfx.core.configs.loader.ConfigAutoLoader;
import rfx.core.stream.node.MasterNode;

public class MasterNodeStarter {	
	public static void main(String[] args) {
		ConfigAutoLoader.loadAll();
		ClusterInfoConfigs configs = ClusterInfoConfigs.load();
		new MasterNode(configs.getMasterHostname(), configs.getMasterHttpPort(),configs.getMasterWebSocketPort()).run();
	}
}