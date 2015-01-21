package rfx.sample.social.tracking;

import rfx.core.stream.functor.common.HttpEventLogTokenizing;
import rfx.core.stream.topology.BaseTopology;
import rfx.core.stream.topology.Pipeline;
import rfx.core.util.LogUtil;
import rfx.core.util.Utils;
import rfx.sample.social.tracking.functor.SocialTrendsFinding;
import rfx.sample.social.tracking.functor.UserSocialActivityCounting;


/**
 * tracking user activity and finding social trends (keywords, topics) in real-time
 * @author trieu
 */
public class TrackingUserTopology extends BaseTopology  {	
	private static final String TOPIC_SOCIAL_ACTIVITY = "social-activity";		
		
	@Override
	public BaseTopology buildTopology(){				
		return Pipeline.create(this)
				.apply(HttpEventLogTokenizing.class)
				.apply(UserSocialActivityCounting.class)
				.apply(SocialTrendsFinding.class)	
				.done();
	}
	
	public static void main(String[] args) {
		LogUtil.setPrefixFileName(TOPIC_SOCIAL_ACTIVITY);
		int beginPartitionId  = 0;
		int endPartitionId  = 1;		
		new TrackingUserTopology().initKafkaDataSeeders(TOPIC_SOCIAL_ACTIVITY, beginPartitionId, endPartitionId)
		.buildTopology().start();
		Utils.sleep(2000);
	}
}