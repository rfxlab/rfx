package rfx.core.stream.kafka;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;

import org.mapdb.DB;

import rfx.core.stream.connector.MapDbConnector;
import rfx.core.stream.kafka.KafkaDataQuery.QueryFilter;
import rfx.core.stream.message.KafkaDataPayload;
import rfx.core.util.StringUtil;
import rfx.core.util.Utils;

public class KafkaDataSeeder {
    private static Map<String, MapDbConnector> mapDbTopic = new HashMap<>();

    public static synchronized void updateOffset(String topic, Map<String, Long> offsets, String workerName) {
		MapDbConnector connector = loadMapDbForTopic(topic,workerName);
		DB mapDb = connector.getMapDb();
		ConcurrentNavigableMap<String, Long> kafkaOffsetDb = connector.getOffsetMapDb();
	
		try {
		    for (Map.Entry<String, Long> entry : offsets.entrySet()) {
		    	kafkaOffsetDb.put(entry.getKey(), entry.getValue());
		    }
		} catch (Exception e) {
		    System.out.println("Oops:" + e);
		    e.printStackTrace();
		} finally {
		    // commit MapDB data to disk
		    mapDb.commit();
		}
    }

    public static synchronized Map<String, Long> getOffsets(String topic, String workerName) {
		MapDbConnector connector = loadMapDbForTopic(topic, workerName);
		ConcurrentNavigableMap<String, Long> kafkaOffsetDb = connector.getOffsetMapDb();
		return kafkaOffsetDb;
    }

    public static synchronized MapDbConnector loadMapDbForTopic(String topic, String workerName) {
		MapDbConnector connector = mapDbTopic.get(topic);
		if (connector == null) {
		    connector = new MapDbConnector(topic,workerName);
		    mapDbTopic.put(topic, connector);
		}
		return connector;
    }
    
    public static synchronized void shutdownMapDB() {
  		Collection<MapDbConnector> connectors = mapDbTopic.values();
  		for (MapDbConnector mapDbConnector : connectors) {
		    mapDbConnector.shutdown();
		}
    }
    

    public static int stopSeedingAndWait(List<KafkaDataSeeder> dataSeeders) {
		for (KafkaDataSeeder dataSeeder : dataSeeders) {
		    dataSeeder.setStopSeedingData(true);
		}
		for (KafkaDataSeeder dataSeeder : dataSeeders) {
		    // wait for all dataSeeders have nothing to seed to system, then stop
		    int times = 4;// retry for 6 times and break
		    while (dataSeeder.getSeededDataSize() > 0) {
				Utils.sleep(500);
				times--;
				if (times == 0) {
				    break;
				}
		    }
		}    
		return dataSeeders.size();
    }

    private String topic;
    int partition, seededDataSize;
    List<String> seeds;
    private boolean stopSeedingData;
    private KafkaDataQuery query;
    private String workerName = "";
    
    MapDbConnector mapDbConnector;
    DB mapDb;
    

    public KafkaDataSeeder(String topic, int partition) {
		super();
		this.topic = topic;
		this.partition = partition;
		this.seeds = new ArrayList<>(0);
		// mapDb loading
		mapDbConnector = loadMapDbForTopic(topic,workerName);
		mapDb = mapDbConnector.getMapDb();
    }
   

    public KafkaDataSeeder buildQuery() {
		if (query == null) {
		    query = new KafkaDataQuery(topic, partition, seeds);
		}
		return this;
    }

    public KafkaDataSeeder buildQuery(QueryFilter queryFilter) {
		if (query == null) {
		    query = new KafkaDataQuery(topic, partition, seeds);
		}
		query.setQueryFilter(queryFilter);
		return this;
    }

    public boolean isStopSeedingData() {
    	return stopSeedingData;
    }

    public void setStopSeedingData(boolean stopSeedingData) {
    	this.stopSeedingData = stopSeedingData;
    }

    public int getSeededDataSize() {
    	return seededDataSize;
    }

    public String getTopic() {
	return topic;
    }

    public int getPartition() {
    	return partition;
    }

    public List<String> getSeeds() {
    	return seeds;
    }

    public KafkaDataQuery getQuery() {
    	return query;
    }

    public synchronized KafkaDataPayload seedData() {
		if (isStopSeedingData()) {
		    this.seededDataSize = 0;
		    return null;
		}
		buildQuery();
	
		
		ConcurrentNavigableMap<String, Long> kafkaOffsetDb = mapDbConnector.getOffsetMapDb();
	
		KafkaDataPayload dataPayload = null;
		try {
		    KafkaDataSource kafkaDataSource = new KafkaDataSource();
	
		    // load from MapDB
		    String clientName = query.buildClientName();
		    if (kafkaOffsetDb.containsKey(clientName)) {
		    	long offset = StringUtil.safeParseLong(kafkaOffsetDb.get(clientName));
				query.setRecentReadOffset(offset);
		    }
	
		    // query kafka
		    dataPayload = kafkaDataSource.query(query);
	
		    if (dataPayload != null) {     
				this.seededDataSize = dataPayload.size();
				long readOffset = dataPayload.getEndOffset();
				
				query.setRecentReadOffset(readOffset);
		
				// save offset to MapDB
				Utils.sleep(1);
				kafkaOffsetDb.put(clientName, readOffset);
		
				System.out.println(new Date() + " ,topic:" + topic+ " ,partition:" + partition + " ,offset:" + readOffset + " size:" + this.seededDataSize);
		    }
		} catch (Exception e) {
		    System.out.println("Oops:" + e);
		    e.printStackTrace();
		} finally {
		    // commit MapDB data to disk
		    mapDb.commit();
		    Utils.sleep(1);
		}
		return dataPayload;
    }
    
    public static List<KafkaDataSeeder> buildKafkaDataSeeder(int beginPartitionId, int endPartitionId, String topic){
    	// inject params to topo
    	List<KafkaDataSeeder> dataSeeders = new ArrayList<>();
    	for (int partition = beginPartitionId; partition <= endPartitionId; partition++) {
    		dataSeeders.add(new KafkaDataSeeder(topic, partition));
    		System.out.println("---create KafkaDataSeeder:"
    				+ StringUtil.toString(topic, "-", partition));
    	}
    	return dataSeeders;
    }
	
}