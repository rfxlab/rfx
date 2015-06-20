package system.starter;

import rfx.core.configs.WorkerConfigs;
import rfx.core.configs.loader.ConfigAutoLoader;
import rfx.core.model.WorkerInfo;
import rfx.core.stream.node.MasterNode;

public class MasterNodeStarter {	
	public static void main(String[] args) {
		ConfigAutoLoader.loadAll();
		WorkerInfo workerInfo = WorkerConfigs.load().getAllocatedWorkers().get(0);
		new MasterNode(workerInfo.getHost(), workerInfo.getPort(),workerInfo.getPort()+1).run();
	}
}