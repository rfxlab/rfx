package rfx.core.stream.kafka;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.OffsetRequest;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

import org.apache.commons.lang3.exception.ExceptionUtils;

import rfx.core.stream.configs.KafkaTopologyConfig;
import rfx.core.stream.message.KafkaDataPayload;
import rfx.core.util.LogUtil;
import rfx.core.util.StringPool;
import rfx.core.util.StringUtil;
import rfx.core.util.Utils;

/**
 * 
 * the main class for connecting Kafka brokers and getting stream logs
 * 
 * @author trieu
 *
 */
public class KafkaDataSource {
	
	static final String TAG = KafkaDataSource.class.getSimpleName();
	static String errorMsg = "OffsetOutOfRange: The requested offset is outside the range of offsets maintained by the server for the given topic/partition.";
	
	private List<String> m_replicaBrokers = new ArrayList<String>();

	public KafkaDataSource() {
		m_replicaBrokers = new ArrayList<String>();
	}
	
	public KafkaDataPayload query(KafkaDataQuery kq) {
		int bufferSize = kq.getBufferForOneFetch();
		int a_partition = kq.getPartition();
		String a_topic = kq.getTopic();
		String clientName = kq.buildClientName();
		long beginOffset = kq.getRecentReadOffset(),messageOffset = beginOffset;		
					
		List<KafkaData> kafkaData = new ArrayList<>();		
		SimpleConsumer consumer = null;
        try {
        	List<String> brokers = KafkaTopologyConfig.getBrokerList(a_topic);
			PartitionMetadata partitionMetadata = KafkaClusterUtil.findLeader(brokers, a_topic, a_partition);
			if (partitionMetadata == null) {
			    LogUtil.e(TAG, "partitionMetadata == null, can't find metadata for Topic and Partition. Exiting");
			    return new KafkaDataPayload(a_topic, a_partition);
			} else if(partitionMetadata.leader() == null){
				partitionMetadata = KafkaClusterUtil.findLeader(brokers, a_topic, a_partition);
				if (partitionMetadata.leader() == null) {
				    LogUtil.e(TAG, "partitionMetadata.leader() == null, can't find metadata for Topic and Partition. Exiting");
				    return new KafkaDataPayload(a_topic, a_partition);
				}
			}
			Broker leader = partitionMetadata.leader();
			String leaderHost = leader.host();
			int leaderPort = leader.port();
			consumer = new SimpleConsumer(leaderHost, leaderPort, 100000, bufferSize , clientName);
					
			if(messageOffset  < 0 ){
				//the first time request to kafka, we ask zookeeper for the earliest time our system got logs
				long whichtime = kafka.api.OffsetRequest.EarliestTime();	
				messageOffset = KafkaClusterUtil.getLastOffset(consumer, a_topic, a_partition, whichtime , clientName);
			}
						
			FetchRequest req = new FetchRequestBuilder()
			        .clientId(clientName)
			        .addFetch(a_topic, a_partition, messageOffset, bufferSize)			        
			        .build();
			FetchResponse fetchResponse = consumer.fetch(req);

			if (fetchResponse.hasError()) {			 
			    // Something went wrong!
			    short code = fetchResponse.errorCode(a_topic, a_partition);
			    if(code == 1 || code == ErrorMapping.OffsetOutOfRangeCode()){
			    	//https://cwiki.apache.org/KAFKA/a-guide-to-the-kafka-protocol.html#AGuideToTheKafkaProtocol-OffsetAPI
			    	
			    	LogUtil.e(TAG,"Error fetching data from the Broker:" + leaderHost + " Reason: " + errorMsg);
			    	
			    	//try 100 times  for searching valid offset			    	
			    	for (int i = 0; i < 100; i+=1) {
			    		messageOffset++;//try to get next log
				    	fetchResponse = consumer.fetch(new FetchRequestBuilder()
				        .clientId(clientName)
				        .addFetch(a_topic, a_partition, messageOffset, bufferSize)			        
				        .build());
				    	if ( fetchResponse.hasError() ) {
				    		Utils.sleep(100);
				    	} else {
				    		break;
				    	}
					}			    
			    	
			    	//give up, find the latest 
			    	if (fetchResponse.hasError()) {		
			    		code = fetchResponse.errorCode(a_topic, a_partition);
			    		if (code == ErrorMapping.OffsetOutOfRangeCode())  {
					    	messageOffset = KafkaClusterUtil.getLastOffset(consumer,a_topic, a_partition, kafka.api.OffsetRequest.LatestTime(), clientName);
					        kq.setRecentReadOffset(messageOffset);							
							if (consumer != null) consumer.close();
							LogUtil.e("UPDATE_READ_OFFSET", messageOffset + " from LatestTime at currentTimeMillis " + System.currentTimeMillis());
							return new KafkaDataPayload(kafkaData, a_topic, a_partition, beginOffset, messageOffset);
			    		}
			    	}
			    }
			    			    
			    try {
					leaderHost = KafkaClusterUtil.findNewLeader(leaderHost, a_topic, a_partition, leaderPort);
				} catch (Exception e) {
					//resetOffsetToCurrent(consumer);
					//FIXME
				} finally {
					if (consumer != null) consumer.close(); consumer = null;
				}
			    return new KafkaDataPayload(a_topic, a_partition);
			}
			
			//get messages from valid fetchResponse
			for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(a_topic, a_partition)) {
			    ByteBuffer payload = messageAndOffset.message().payload();
			    messageOffset = messageAndOffset.nextOffset();
			    byte[] bytes = new byte[payload.limit()];
			    payload.get(bytes);
				String data =  new String(bytes, StringPool.UTF_8);
				kafkaData.add(new KafkaData(messageOffset, data));
			}
			
			if(kafkaData.size() > 0)
			{
				LogUtil.d("kafkaSpout, topic:"+a_topic+" seedMessage at offset " + messageOffset + " kafkaData.size(): " + kafkaData.size());
				kq.setRecentReadOffset(messageOffset);
			}
		} catch (Throwable e) {
			e.printStackTrace();			
			LogUtil.e("KafkaDataSource", ExceptionUtils.getStackTrace(e));			
		}  finally {
			if (consumer != null) consumer.close();
		}
        return new KafkaDataPayload(kafkaData, a_topic, a_partition, beginOffset, messageOffset);
    }
	

	public KafkaDataPayload queryFromSeedBrokers(KafkaDataQuery  kq)  {
		long a_maxReads = kq.getMaxReads();		
		int bufferForOneFetch = kq.getBufferForOneFetch();
		int a_partition = kq.getPartition();
		String a_topic = kq.getTopic();
		long readOffset = kq.getRecentReadOffset();
		String clientName = kq.buildClientName();
		
		PartitionMetadata metadata = findLeader(kq.getSeedBrokers(), a_topic, a_partition);
		if (metadata == null) {			
			LogUtil.e("KafkaDataSource.query", "Can't find metadata for Topic and Partition");
			return new KafkaDataPayload(a_topic, a_partition);
		}
		if (metadata.leader() == null) {			
			LogUtil.e("KafkaDataSource.query", "Can't find Leader for Topic and Partition");
			return new KafkaDataPayload(a_topic, a_partition);
		}
		
		String leadBrokerHost = metadata.leader().host();
		int leadBrokerPort = metadata.leader().port();
				
		SimpleConsumer consumer = new SimpleConsumer(leadBrokerHost, leadBrokerPort,100000, 64 * 1024, clientName);		
		
		if(readOffset < 0){
			readOffset = getLastOffset(consumer, a_topic, a_partition, OffsetRequest.EarliestTime(), clientName);
		}
		long beginOffset = readOffset, messageOffset = readOffset;
		List<KafkaData> kafkaDataList = new ArrayList<>();
		int numErrors = 0;
		
		try {
			while (true) {
				if (consumer == null) {
					consumer = new SimpleConsumer(leadBrokerHost, leadBrokerPort, 100000, 64 * 1024, clientName);
				}
				FetchRequestBuilder requestBuilder = new FetchRequestBuilder().clientId(clientName);
				FetchRequest req = requestBuilder.addFetch(a_topic, a_partition, readOffset, bufferForOneFetch).build();
				FetchResponse fetchResponse = consumer.fetch(req);

				if (fetchResponse.hasError()) {
					numErrors++;
					// Something went wrong!
					short code = fetchResponse.errorCode(a_topic, a_partition);					
					LogUtil.e("KafkaDataSource.query", "Error fetching data from the Broker:"+ leadBrokerHost + " Reason: " + code);
					if (numErrors > 5){
						break;
					}
					if (code == ErrorMapping.OffsetOutOfRangeCode()) {
						// We asked for an invalid offset. For simple case ask for the last element to reset
						readOffset = getLastOffset(consumer, a_topic, a_partition,OffsetRequest.LatestTime(), clientName);
						continue;
					}
					consumer.close();					
					leadBrokerHost = findNewLeader(leadBrokerHost, a_topic, a_partition, leadBrokerPort);
					continue;
				}
				numErrors = 0;
				
				long numRead = 0;
				for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(a_topic, a_partition)) {
					long currentOffset = messageAndOffset.offset();
					if (currentOffset < readOffset) {						
						LogUtil.e("KafkaDataSource.query", "Found an old offset: " + currentOffset	+ " Expecting: " + readOffset);
						continue;
					}
					readOffset = messageAndOffset.nextOffset();
					ByteBuffer payload = messageAndOffset.message().payload();

					byte[] bytes = new byte[payload.limit()];
					payload.get(bytes);
					
					messageOffset = messageAndOffset.offset();
					if(messageOffset > beginOffset){
						String data = new String(bytes, "UTF-8");
						if(kq.queryFilter(messageOffset, data)){
							kafkaDataList.add(new KafkaData(messageOffset, data));
						}
					}
					
					numRead++;				
					if(a_maxReads > 0){
						a_maxReads--;
					}
				}
				
				if(a_maxReads <= 0 || numRead == 0){
					break;
				}
			}
		} catch (Exception e) {		
			e.printStackTrace();
			LogUtil.e("KafkaDataSource.query", e.getMessage());
		} finally {
			if (consumer != null){
				consumer.close();
			}
		}
		return new KafkaDataPayload(kafkaDataList, a_topic, a_partition, beginOffset, messageOffset);
	}

	public static long getLastOffset(SimpleConsumer consumer, String topic,
			int partition, long whichTime, String clientName) {
		TopicAndPartition topicAndPartition = new TopicAndPartition(topic,
				partition);
		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
		kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(),clientName);
		OffsetResponse response = consumer.getOffsetsBefore(request);

		if (response.hasError()) {			
			LogUtil.e("KafkaDataSource.getLastOffset", "Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition));
			return 0;
		}
		long[] offsets = response.offsets(topic, partition);
		return offsets[0];
	}

	private String findNewLeader(String a_oldLeader, String a_topic,
			int a_partition, int a_port) throws Exception {
		for (int i = 0; i < 3; i++) {
			boolean goToSleep = false;
			PartitionMetadata metadata = findLeader(m_replicaBrokers, a_port, a_topic, a_partition);
			if (metadata == null) {
				goToSleep = true;
			} else if (metadata.leader() == null) {
				goToSleep = true;
			} else if (a_oldLeader.equalsIgnoreCase(metadata.leader().host())
					&& i == 0) {
				// first time through if the leader hasn't changed give
				// ZooKeeper a second to recover
				// second time, assume the broker did recover before failover,
				// or it was a non-Broker issue
				//
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
		LogUtil.e("KafkaDataSource.getLastOffset", "Unable to find new leader after Broker failure. Exiting");
		throw new Exception("Unable to find new leader after Broker failure. Exiting");
	}

	private PartitionMetadata findLeader(List<String> a_seedBrokers,
			int a_port, String a_topic, int a_partition) {
		PartitionMetadata returnMetaData = null;
		for (String seed : a_seedBrokers) {
			SimpleConsumer consumer = null;
			try {
				consumer = new SimpleConsumer(seed, a_port, 100000, 64 * 1024,"leaderLookup");
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
				LogUtil.e("KafkaDataSource.getLastOffset", "Error communicating with Broker [" + seed
						+ "] to find Leader for [" + a_topic + ", "
						+ a_partition + "] Reason: " + e);
			} finally {
				if (consumer != null){
					consumer.close();
				}
			}
		}
		if (returnMetaData != null) {
			m_replicaBrokers.clear();
			for (kafka.cluster.Broker replica : returnMetaData.replicas()) {
				m_replicaBrokers.add(replica.host());
			}
		}
		return returnMetaData;
	}
	
	public PartitionMetadata findLeader(List<String> a_seedBrokers,String a_topic, int a_partition) {
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
				LogUtil.e("KafkaDataSource.getLastOffset", "Error communicating with Broker [" + seed
						+ "] to find Leader for [" + a_topic + ", "
						+ a_partition + "] Reason: " + e);
			} finally {
				if (consumer != null){
					consumer.close();
				}
			}
		}
		if (returnMetaData != null) {
			m_replicaBrokers.clear();
			for (kafka.cluster.Broker replica : returnMetaData.replicas()) {
				m_replicaBrokers.add(replica.host());
			}
		}
		return returnMetaData;
	}
}
