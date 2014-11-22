package rfx.sample.social.tracking;

import rfx.core.stream.functor.common.TokenizingDefaultEventLog;
import rfx.core.stream.topology.BaseTopology;
import rfx.core.stream.topology.Pipeline;
import rfx.core.util.LogUtil;
import rfx.core.util.Utils;
import rfx.sample.social.tracking.functor.FindingSocialTrends;
import rfx.sample.social.tracking.functor.ParsingSocialActivityLog;


/**
 * tracking user activity and finding social trends (keywords, topics) in real-time
 * @author trieu
 */
public class TrackingUserTopology extends BaseTopology  {	
	private static final String TOPIC_SOCIAL_ACTIVITY = "social-activity";		
		
	@Override
	public BaseTopology buildTopology(){
		LogUtil.setPrefixFileName(TOPIC_SOCIAL_ACTIVITY);		
		return Pipeline.create(this)
				.apply(TokenizingDefaultEventLog.class)
				.apply(ParsingSocialActivityLog.class)
				.apply(FindingSocialTrends.class)	
				.done();
	}
	
	public static void main(String[] args) {
		int beginPartitionId  = 0;
		int endPartitionId  = 1;		
		new TrackingUserTopology().initKafkaDataSeeders(TOPIC_SOCIAL_ACTIVITY, beginPartitionId, endPartitionId)
		.buildTopology().start();
		Utils.sleep(2000);
	}
}