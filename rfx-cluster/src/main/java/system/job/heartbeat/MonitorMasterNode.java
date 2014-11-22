package system.job.heartbeat;

import rfx.core.job.ScheduledJob;
import rfx.core.util.Utils;

public class MonitorMasterNode extends ScheduledJob {

	@Override
	public void doTheJob() {
		Utils.sleep(1000);
		System.out.println("PING master node");
		//TODO
		Utils.sleep(1000);
	}

}

