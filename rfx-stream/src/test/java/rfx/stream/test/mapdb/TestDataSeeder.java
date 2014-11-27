package rfx.stream.test.mapdb;

import java.util.concurrent.ConcurrentNavigableMap;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mapdb.DB;

import rfx.core.stream.connector.MapDbConnector;
import rfx.core.stream.kafka.KafkaDataSeeder;
import rfx.core.util.Utils;


public class TestDataSeeder {

	@Before
	public void initData() {
		System.out.println("initData");
		MapDbConnector connector = KafkaDataSeeder.loadMapDbForTopic("my-topic", "");
		DB mapDb = connector.getMapDb();
		ConcurrentNavigableMap<String, Long> kafkaOffsetDb = connector.getOffsetMapDb();

		String clientName = "client";
		long readOffset = -1;
		for (readOffset = 1; readOffset < 10000; readOffset++) {
			// save offset to MapDB
			kafkaOffsetDb.put(clientName, readOffset);
			Utils.sleep(1);
		}
		Assert.assertEquals(1, kafkaOffsetDb.size());
		Assert.assertEquals(10000, readOffset);
		mapDb.commit();
		Utils.sleep(500);
	}

	@Test
	public void checkData() {
		System.out.println("checkData");
		MapDbConnector connector = KafkaDataSeeder.loadMapDbForTopic(
				"my-topic", "");
		DB mapDb = connector.getMapDb();
		ConcurrentNavigableMap<String, Long> kafkaOffsetDb = connector
				.getOffsetMapDb();

		String clientName = "client";
		long offset = kafkaOffsetDb.getOrDefault(clientName, -1l);
		System.out.println(offset);
		Assert.assertEquals(9999, offset);
		mapDb.commit();
		Utils.sleep(500);
		mapDb.close();
		Utils.sleep(500);
	}

}
