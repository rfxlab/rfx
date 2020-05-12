package server.kafka;


import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import kafka.utils.VerifiableProperties;

public class SimplePartitioner implements Partitioner {

	static final int MIN = 0;

	public SimplePartitioner() {

	}

	public SimplePartitioner(VerifiableProperties props) {
		// System.out.println(props);
	}

	

	@Override
	public void configure(Map<String, ?> configs) {
		// TODO Auto-generated method stub
		
	}

	/*
	 * Randomly write to kafka
	 */
	@Override
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes,Cluster cluster) {
		int partition = 0;
		int MAX = cluster.partitionCountForTopic(topic) - 1;
		partition = MIN + (int) (Math.random() * ((MAX - MIN) + 1));
		//System.out.println("a_numPartitions: " + a_numPartitions + " partitionId: " + partition);
		return partition;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

}
