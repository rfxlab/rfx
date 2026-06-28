package rfx.core.util.test;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;
import java.util.UUID;



import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPooled;
import rfx.core.configs.RedisConfigs;
import rfx.core.nosql.jedis.RedisInfo;

public class RedisIntegrationTest {

    public static void main(String[] args) {

        RedisConfigs configs = RedisConfigs.load();

        Map<String, List<RedisInfo>> pools = configs.getRedisPools();

        assertFalse(pools.isEmpty());

        pools.forEach((poolName, servers) -> {

            assertFalse(servers.isEmpty(), poolName + " has no Redis server");

            for (RedisInfo info : servers) {

                DefaultJedisClientConfig.Builder builder =
                        DefaultJedisClientConfig.builder();

                if (info.getUsername() != null) {
                    builder.user(info.getUsername());
                }

                if (info.getPassword() != null) {
                    builder.password(info.getPassword());
                }

                builder.ssl(info.isUseSsl());

                try (JedisPooled jedis = new JedisPooled(
                        new HostAndPort(info.getHost(), info.getPort()),
                        builder.build())) {

                    //-----------------------------------
                    // Connection
                    //-----------------------------------
                    assertEquals("PONG", jedis.ping(),
                            "Cannot connect to " + poolName);

                    //-----------------------------------
                    // Read / Write
                    //-----------------------------------
                    String key = "unit-test:" + UUID.randomUUID();
                    String value = UUID.randomUUID().toString();

                    jedis.set(key, value);

                    assertEquals(value, jedis.get(key));

                    assertTrue(jedis.exists(key));

                    jedis.del(key);

                    assertFalse(jedis.exists(key));

                    System.out.printf(
                            "[PASS] %-20s %s:%d%n",
                            poolName,
                            info.getHost(),
                            info.getPort());

                } catch (Exception e) {
                    fail(String.format(
                            "Cannot connect to Redis pool '%s' (%s:%d): %s",
                            poolName,
                            info.getHost(),
                            info.getPort(),
                            e.getMessage()));
                }
            }
        });
    }

}
