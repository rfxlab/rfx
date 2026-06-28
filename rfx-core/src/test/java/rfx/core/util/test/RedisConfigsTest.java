package rfx.core.util.test;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import rfx.core.configs.RedisConfigs;
import rfx.core.nosql.jedis.RedisInfo;
import rfx.core.nosql.jedis.Shardable;
import rfx.core.util.CommonUtil;

public class RedisConfigsTest {

    @TempDir
    Path tempDir;

    @BeforeEach
    public void setup() throws Exception {

        String json = "{\n" +
                "  \"default\": [\n" +
                "    {\n" +
                "      \"host\":\"127.0.0.1\",\n" +
                "      \"port\":6379\n" +
                "    }\n" +
                "  ],\n" +
                "  \"cluster\": [\n" +
                "    {\n" +
                "      \"host\":\"10.0.0.1\",\n" +
                "      \"port\":6380,\n" +
                "      \"username\":\"admin\",\n" +
                "      \"password\":\"secret\",\n" +
                "      \"ssl\":true\n" +
                "    },\n" +
                "    {\n" +
                "      \"host\":\"10.0.0.2\",\n" +
                "      \"port\":6381\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        Path config = tempDir.resolve("redis.json");
        Files.writeString(config, json);

        System.setProperty(CommonUtil.getRedisConfigFile(), config.toString());

        // Reset singleton
        Field instance = RedisConfigs.class.getDeclaredField("instance");
        instance.setAccessible(true);
        instance.set(null, null);
    }

    @Test
    public void testLoadConfigs() {

        RedisConfigs configs = RedisConfigs.load();

        Map<String, List<RedisInfo>> pools = configs.getRedisPools();

        assertEquals(2, pools.size());

        assertTrue(pools.containsKey("default"));
        assertTrue(pools.containsKey("cluster"));
    }

    @Test
    public void testDefaultRedis() {

        RedisInfo redis = RedisConfigs.load().get("default");

        assertEquals("127.0.0.1", redis.getHost());
        assertEquals(6379, redis.getPort());

        assertNull(redis.getUsername());
        assertNull(redis.getPassword());
        assertFalse(redis.isUseSsl());
    }

    @Test
    public void testAclRedis() {

        RedisInfo redis = RedisConfigs.load().get("cluster");

        assertEquals("10.0.0.1", redis.getHost());
        assertEquals(6380, redis.getPort());

        assertEquals("admin", redis.getUsername());
        assertEquals("secret", redis.getPassword());
        assertTrue(redis.isUseSsl());
    }

    @Test
    public void testGetAllMatchedPools() {

        List<RedisInfo> list = RedisConfigs.load().getAllMatchedPools("cluster");

        assertEquals(2, list.size());

        assertEquals("10.0.0.1", list.get(0).getHost());
        assertEquals("10.0.0.2", list.get(1).getHost());
    }

    @Test
    public void testShardByLongKey() {

        RedisConfigs configs = RedisConfigs.load();

        RedisInfo redis0 = configs.getBySharding("cluster", 0);
        RedisInfo redis1 = configs.getBySharding("cluster", 1);
        RedisInfo redis2 = configs.getBySharding("cluster", 2);

        assertEquals("10.0.0.1", redis0.getHost());
        assertEquals("10.0.0.2", redis1.getHost());
        assertEquals("10.0.0.1", redis2.getHost());
    }

    @Test
    public void testShardByObject() {

        RedisConfigs configs = RedisConfigs.load();

        Shardable s = new Shardable() {

            @Override
            public Long getShardKey() {
                return 3L;
            }
        };

        RedisInfo redis = configs.getBySharding("cluster", s);

        assertEquals("10.0.0.2", redis.getHost());
    }

    @Test
    public void testUnknownAccessKey() {

        RedisConfigs configs = RedisConfigs.load();

        assertThrows(IllegalArgumentException.class,
                () -> configs.get("not-found"));

        assertThrows(IllegalArgumentException.class,
                () -> configs.getAllMatchedPools("not-found"));

        assertThrows(IllegalArgumentException.class,
                () -> configs.getBySharding("not-found", 1L));
    }

    @Test
    public void testPoolsAreImmutable() {

        RedisConfigs configs = RedisConfigs.load();

        assertThrows(UnsupportedOperationException.class,
                () -> configs.getRedisPools().put("x", List.of()));

        assertThrows(UnsupportedOperationException.class,
                () -> configs.getAllMatchedPools("cluster")
                        .add(new RedisInfo("localhost", 6379)));
    }

    @Test
    public void testRedisInfoEqualsAndHashCode() {

        RedisInfo a = new RedisInfo("127.0.0.1", 6379);
        RedisInfo b = new RedisInfo("localhost", 6379);
        RedisInfo c = new RedisInfo("::1", 6379);

        assertEquals(a, b);
        assertEquals(b, c);

        assertEquals(a.hashCode(), b.hashCode());
        assertEquals(b.hashCode(), c.hashCode());
    }

    @Test
    public void testRedisInfoToString() {

        RedisInfo info = new RedisInfo("redis-server", 6380);

        assertEquals("redis-server:6380", info.toString());
    }

}