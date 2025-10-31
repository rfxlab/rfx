package rfx.core.stream.cluster;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.gson.Gson;

import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPooled;
import rfx.core.configs.RedisConfigs;
import rfx.core.configs.WorkerConfigs;
import rfx.core.model.WorkerData;
import rfx.core.model.WorkerInfo;
import rfx.core.model.WorkerTimeLog;
import rfx.core.nosql.jedis.RedisInfo;
import rfx.core.util.LogUtil;
import rfx.core.util.StringUtil;

public class ClusterDataManager {


    public static final String CLUSTER_WORKER_PREFIX = "workers";
    public static final String WORKER_INFO_POSTFIX = ".info";
    public static final String WORKER_DATA_POSTFIX = ".data";
    public static final String WORKER_TIMELOG_POSTFIX = ".timelog";

    static final WorkerConfigs workerConfigs = WorkerConfigs.load();

    /** one pooled, thread-safe client shared across app */
    private static final JedisPooled redisClient = buildRedisClient();

    private static JedisPooled buildRedisClient() {
    	RedisInfo redisInfo = RedisConfigs.load().get("clusterInfoRedis");
        String host = redisInfo.getHost();
        int port = redisInfo.getPort();
        String auth = redisInfo.getAuth();

        DefaultJedisClientConfig.Builder cfg = DefaultJedisClientConfig.builder()
                .connectionTimeoutMillis(2000)
                .socketTimeoutMillis(2000);
        if (auth != null && !auth.isEmpty()) cfg.password(auth);

        return new JedisPooled(new HostAndPort(host, port), cfg.build());
    }

    public static JedisPooled getJedisClient() {
        return redisClient;
    }

    // ------------------------------------------------------------------------

    public static boolean saveWorkerInfo(WorkerInfo workerInfo) {
        try {
            redisClient.hset(CLUSTER_WORKER_PREFIX,
                    workerInfo.getName() + WORKER_INFO_POSTFIX,
                    workerInfo.toJson());
            return true;
        } catch (Exception e) {
            LogUtil.error(e);
            return false;
        }
    }

    public static Map<String, WorkerInfo> getWorkerInfoFromRedis() {
        Map<String, WorkerInfo> mapWorkerInfo = new HashMap<>();
        try {
            Map<String, String> map = redisClient.hgetAll(CLUSTER_WORKER_PREFIX);
            for (Map.Entry<String, String> entry : map.entrySet()) {
                String key = entry.getKey();
                if (key.endsWith(WORKER_INFO_POSTFIX)) {
                    String json = entry.getValue();
                    if (StringUtil.isNotEmpty(json)) {
                        String name = key.replace(WORKER_INFO_POSTFIX, "");
                        mapWorkerInfo.put(name, WorkerInfo.fromJson(json));
                    }
                }
            }
        } catch (Exception e) {
            LogUtil.error(e);
        }
        return mapWorkerInfo;
    }

    public static boolean ping(WorkerInfo workerInfo) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(workerInfo.getHost(), workerInfo.getPort()), 300);
            return true;
        } catch (Exception ex) {
            return false;
        }
    }

    public static List<WorkerData> getWorkerData() {
        Map<String, String> map;
        try {
            map = redisClient.hgetAll(CLUSTER_WORKER_PREFIX);
        } catch (Exception e) {
            LogUtil.error(e);
            return List.of();
        }

        List<WorkerData> datas = new ArrayList<>(map.size() / 3);
        Gson gson = new Gson();

        for (String key : map.keySet()) {
            if (key.endsWith(WORKER_DATA_POSTFIX)) {
                String jsonData = map.get(key);
                String jsonInfo = map.get(key.replace(WORKER_DATA_POSTFIX, WORKER_INFO_POSTFIX));
                String jsonTimeLog = map.get(key.replace(WORKER_DATA_POSTFIX, WORKER_TIMELOG_POSTFIX));
                if (jsonData != null) {
                    WorkerData workerData = gson.fromJson(jsonData, WorkerData.class);
                    WorkerInfo workerInfo = gson.fromJson(jsonInfo, WorkerInfo.class);
                    WorkerTimeLog workerTimeLog = gson.fromJson(jsonTimeLog, WorkerTimeLog.class);

                    workerData.setHostname(workerInfo.getHost() + ":" + workerInfo.getPort());
                    workerData.setStatus(workerInfo.isAlive() ? "ALIVE" : "DIED");

                    long upTime = workerTimeLog.getLastUpTime();
                    long downTime = workerTimeLog.getLastDownTime();
                    if (upTime > downTime) {
                        long upTimeAmount = System.currentTimeMillis() - upTime;
                        long hours = TimeUnit.MILLISECONDS.toHours(upTimeAmount);
                        long minutes = TimeUnit.MILLISECONDS.toMinutes(upTimeAmount) % 60;
                        long seconds = TimeUnit.MILLISECONDS.toSeconds(upTimeAmount) % 60;
                        workerData.setUptime(String.format("%d:%02d:%02d", hours, minutes, seconds));
                    } else {
                        workerData.setUptime("0:0:0");
                    }
                    datas.add(workerData);
                }
            }
        }
        return datas;
    }

    /** worker node updates its own memory usage etc. */
    public static void updateWorkerData(String host, int port) {
        try {
            String workerName = StringUtil.toString(host.replaceAll("\\.", ""), "_", port);
            Gson gson = new Gson();

            WorkerData workerData = gson.fromJson(redisClient.hget(CLUSTER_WORKER_PREFIX, workerName + WORKER_DATA_POSTFIX), WorkerData.class);

            if (workerData == null) {
                workerData = new WorkerData();
            }

            Runtime rt = Runtime.getRuntime();
            workerData.setMemory_usage(readableFileSize(rt.totalMemory() - rt.freeMemory()));
            workerData.setMemory_limit(readableFileSize(rt.maxMemory()));

            redisClient.hset(CLUSTER_WORKER_PREFIX,
                    workerName + WORKER_DATA_POSTFIX,
                    gson.toJson(workerData));

        } catch (Exception e) {
            LogUtil.error(e);
        }
    }

    public static String readableFileSize(long size) {
        if (size <= 0) return "0";
        final String[] units = {"B", "KB", "MB", "GB", "TB"};
        int digitGroups = (int) (Math.log10(size) / Math.log10(1024));
        return new DecimalFormat("#,##0.#")
                .format(size / Math.pow(1024, digitGroups)) + " " + units[digitGroups];
    }
}
