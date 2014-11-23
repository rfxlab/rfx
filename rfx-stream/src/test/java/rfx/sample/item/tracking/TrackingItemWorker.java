package rfx.sample.item.tracking;

import rfx.core.stream.model.KafkaTaskDef;
import rfx.core.stream.model.TaskDef;
import rfx.core.stream.worker.StreamProcessingWorker;
import rfx.core.util.LogUtil;

public class TrackingItemWorker extends StreamProcessingWorker {

	public TrackingItemWorker() {
		super(TrackingItemWorker.class.getSimpleName());		
	}

	public static void main(String[] args) {
		int beginPartitionId  = 0;
		int endPartitionId  = 1;
		String topic = "item-tracking";
				
		LogUtil.setDebug(true);
		TaskDef taskDef = new KafkaTaskDef(topic, beginPartitionId, endPartitionId);
		new TrackingItemWorker().setTaskDef(taskDef).start("127.0.0.1", 15000);
	}

}
