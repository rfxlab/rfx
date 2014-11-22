package rfx.core.stream.kafka;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import rfx.core.util.LogUtil;
import rfx.core.util.StringUtil;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;

public class KafkaClusterUtil {
	static final String TAG = KafkaClusterUtil.class.getSimpleName();

	private static List<String> m_replicaBrokers = new ArrayList<String>();


	public static PartitionMetadata findLeaderFromReplicaBrokers(String a_topic, int a_partition) {
		return findLeader(m_replicaBrokers, a_topic, a_partition);
	}

	public static PartitionMetadata findLeader(List<String> a_seedBrokers,	String a_topic, int a_partition) {
		LogUtil.d("KafkaConfigManager.findLeader", a_seedBrokers + " " + a_topic + " " + a_partition + " ", true);
		PartitionMetadata returnMetaData = null;
		for (String seed : a_seedBrokers) {
			SimpleConsumer consumer = null;
			String[] toks = seed.split(":");
			try {
				String host = StringUtil.safeString(toks[0]);
				int a_port = StringUtil.safeParseInt(toks[1]);
				consumer = new SimpleConsumer(host, a_port, 120 * 1000, 64 * 1024,"leaderLookup");
				List<String> topics = new ArrayList<String>();
				topics.add(a_topic);
				TopicMetadataRequest req = new TopicMetadataRequest(topics);
				kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

				List<TopicMetadata> metaData = resp.topicsMetadata();
				for (TopicMetadata item : metaData) {
					for (PartitionMetadata part : item.partitionsMetadata()) {
						if (part.partitionId() == a_partition) {
							returnMetaData = part;
							break;
						}
					}
				}
			} catch (Exception e) {
				System.out.println("Error communicating with Broker [" + seed
						+ "] to find Leader for [" + a_topic + ", "
						+ a_partition + "] Reason: " + e);
			} finally {
				if (consumer != null)
					consumer.close();
			}
		}
		if (returnMetaData != null) {
			m_replicaBrokers.clear();
			for (kafka.cluster.Broker replica : returnMetaData.replicas()) {
				m_replicaBrokers.add(replica.host());
			}
		}
		//LogUtil.i("KafkaConfigManager.findLeader returnMetaData:", returnMetaData.leader().host() + " " + returnMetaData.leader().port(), true);
		return returnMetaData;
	}

	public static String findNewLeader(String a_oldLeader, String a_topic,	int a_partition, int a_port) throws KafkaReadException {
		for (int i = 0; i < 4; i++) {
			boolean goToSleep = false;
			PartitionMetadata metadata = findLeaderFromReplicaBrokers(a_topic, a_partition);
			if (metadata == null) {
				goToSleep = true;
			} else if (metadata.leader() == null) {
				goToSleep = true;
			} else if (a_oldLeader.equalsIgnoreCase(metadata.leader().host()) && i == 0) {
				// first time through if the leader hasn't changed give
				// ZooKeeper a second to recover
				// second time, assume the broker did recover before failover,
				// or it was a non-Broker issue
				goToSleep = true;
			} else {
				return metadata.leader().host();
			}
			if (goToSleep) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException ie) {
				}
			}
		}
		System.err.println("Unable to find new leader after Broker failure. Exiting");
		throw new KafkaReadException("Unable to find new leader after Broker failure. Exiting");
	}

	public static long getLastOffset(SimpleConsumer consumer, String topic,int partition, long whichTime, String clientName) {
		TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
		kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
		OffsetResponse response = consumer.getOffsetsBefore(request);

		if (response.hasError()) {
			System.out.println("Error fetching data Offset Data the Broker. Reason: "+ response.errorCode(topic, partition));
			return 0;
		}
		long[] offsets = response.offsets(topic, partition);
		//System.out.println("#####" + topic + " " + partition + "  " + offsets.length);
		if(offsets.length > 0){
			return offsets[0];
		}
		return -1;
	}
	
	

}
