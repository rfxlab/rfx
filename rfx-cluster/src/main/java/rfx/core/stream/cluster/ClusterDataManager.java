package rfx.core.stream.cluster;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;
import rfx.core.configs.ClusterInfoConfigs;
import rfx.core.configs.WorkerConfigs;
import rfx.core.model.WorkerData;
import rfx.core.model.WorkerInfo;
import rfx.core.model.WorkerTimeLog;
import rfx.core.nosql.jedis.RedisCommand;
import rfx.core.util.LogUtil;
import rfx.core.util.StringPool;
import rfx.core.util.StringUtil;

import com.google.gson.Gson;

public class ClusterDataManager {

    static final String TAG = ClusterDataManager.class.getSimpleName();
    public static final String CLUSTER_WORKER_PREFIX = "workers";
    public static final String WORKER_INFO_POSTFIX = ".info";
    public static final String WORKER_DATA_POSTFIX = ".data";
    public static final String WORKER_TIMELOG_POSTFIX = ".timelog";

    // ------------ configs ------------
    static final ClusterInfoConfigs clusterInfoConfigs = ClusterInfoConfigs.load();
    static final WorkerConfigs workerConfigs = WorkerConfigs.load();

    static ShardedJedisPool commonClusterRedisPool = clusterInfoConfigs.getClusterInfoRedis()
            .getShardedJedisPool();

    public static ShardedJedisPool getRedisClusterInfoPool() {
        return commonClusterRedisPool;
    }

    // //////////////////// static methods for WorkerInfo //////////////////////

    /**
     * 
     * save WorkerInfo of specified worker, supervisor itself will call this
     * 
     * @return boolean
     */
    public static boolean saveWorkerInfo(WorkerInfo workerInfo) {
        ShardedJedisPool jedisPool = getRedisClusterInfoPool();
        ShardedJedis shardedJedis = null;
        boolean ok = false;
        try {
            shardedJedis = jedisPool.getResource();
            Pipeline pipeline = shardedJedis.getShard(StringPool.BLANK).pipelined();
            pipeline.hset(CLUSTER_WORKER_PREFIX, workerInfo.getName() + WORKER_INFO_POSTFIX,
                    workerInfo.toJson());
            pipeline.sync();
            ok = true;
        } catch (Exception e) {
            LogUtil.error(e);
        } finally {
            RedisCommand.freeRedisResource(jedisPool, shardedJedis, ok);
        }
        return ok;
    }

    /**
     * registerWorkerInfo
     * 
     * @return boolean
     */
    public static Map<String, WorkerInfo> getWorkerInfoFromRedis() {
        ShardedJedisPool jedisPool = getRedisClusterInfoPool();
        ShardedJedis shardedJedis = null;
        boolean ok = false;
        Map<String, WorkerInfo> mapWorkerInfo = null;
        try {
            shardedJedis = jedisPool.getResource();
            Jedis jedis = shardedJedis.getShard(StringPool.BLANK);
            Map<String, String> map = jedis.hgetAll(CLUSTER_WORKER_PREFIX);
            Set<String> names = map.keySet();
            mapWorkerInfo = new HashMap<>(names.size());
            for (String name : names) {
                if (name.endsWith(WORKER_INFO_POSTFIX)) {
                    String json = map.get(name);
                    System.out.println(name + " json: " + json);
                    if (StringUtil.isNotEmpty(json)) {
                        name = name.replace(WORKER_INFO_POSTFIX, "");
                        mapWorkerInfo.put(name, WorkerInfo.fromJson(json));
                    }
                }
            }
            ok = true;
        } catch (Exception e) {
            LogUtil.error(e);
        } finally {
            RedisCommand.freeRedisResource(jedisPool, shardedJedis, ok);
        }
        return mapWorkerInfo == null ? new HashMap<String, WorkerInfo>() : mapWorkerInfo;
    }

    public static boolean ping(WorkerInfo workerInfo) {
        try {
            int timeout = 300;
            Socket socket = new Socket();
            socket.connect(new InetSocketAddress(workerInfo.getHost(), workerInfo.getPort()),
                    timeout);
            socket.close();
            return true;
        } catch (Exception ex) {
        }
        return false;
    }

    /**
     * get all (the method for Master node dashboard )
     * 
     * @return
     */
    public static List<WorkerData> getWorkerData() {
        List<WorkerData> datas = null;
        ShardedJedisPool jedisPool = ClusterDataManager.getRedisClusterInfoPool();
        ShardedJedis shardedJedis = null;
        boolean ok = false;
        try {
            shardedJedis = jedisPool.getResource();
            Jedis jedis = shardedJedis.getShard(StringPool.BLANK);
            Map<String, String> map = jedis.hgetAll(CLUSTER_WORKER_PREFIX);
            Set<String> keys = map.keySet();
            datas = new ArrayList<>(keys.size() / 3);

            System.out.println(map);

            for (String key : keys) {
                if (key.endsWith(WORKER_DATA_POSTFIX)) {
                    String jsonData = map.get(key);
                    String jsonInfo = map.get(key.replace(WORKER_DATA_POSTFIX, WORKER_INFO_POSTFIX));
                    String jsonTimeLog = map.get(key.replace(WORKER_DATA_POSTFIX, WORKER_TIMELOG_POSTFIX));
                    if (jsonData != null) {
                        WorkerData workerData = new Gson().fromJson(jsonData, WorkerData.class);
                        WorkerInfo workerInfo = new Gson().fromJson(jsonInfo, WorkerInfo.class);
                        WorkerTimeLog workerTimeLog = new Gson().fromJson(jsonTimeLog, WorkerTimeLog.class);
                        workerData.setHostname(workerInfo.getHost() + ":" + workerInfo.getPort());
                        workerData.setStatus(workerInfo.isAlive() ? "ALIVE" : "DIED");
                        long upTime = workerTimeLog.getLastUpTime();
                        long downTime = workerTimeLog.getLastDownTime();
                        if (upTime > downTime) {
                            long upTimeAmount = System.currentTimeMillis() - upTime;
                            long uptimeHours = TimeUnit.MILLISECONDS.toHours(upTimeAmount);
                            long uptimeMinutes = TimeUnit.MILLISECONDS.toMinutes(upTimeAmount);
                            long uptimeSeconds = TimeUnit.MILLISECONDS.toSeconds(upTimeAmount);
                            if (uptimeMinutes > 0) {
                                uptimeSeconds = uptimeSeconds % (60 * uptimeMinutes);
                            }
                            if (uptimeHours > 0) {
                                uptimeMinutes = uptimeMinutes % (60 * uptimeHours);
                            }

                            String formatedTime = StringUtil.toString(uptimeHours, ":", uptimeMinutes, ":",
                                    uptimeSeconds);
                            workerData.setUptime(formatedTime);
                        } else {
                            workerData.setActor_list("0:0:0");
                        }
                        datas.add(workerData);
                    }
                }
            }
            ok = true;
        } catch (Exception e) {
//            LogUtil.error(e);
            throw new RuntimeException(e);
        } finally {
            RedisCommand.freeRedisResource(jedisPool, shardedJedis, ok);
        }
        return datas == null ? new ArrayList<WorkerData>(0) : datas;
    }

    /**
     * Set worker data (worker itself will call this )
     * 
     * @param host Worker host
     * @param port worker port
     */
    public static void updateWorkerData(String host, int port) {
        ShardedJedisPool jedisPool =  ClusterDataManager.getRedisClusterInfoPool();
        ShardedJedis shardedJedis = null;
        boolean isCommited = false;
        try {
            shardedJedis = jedisPool.getResource();
            Jedis jedis = shardedJedis.getShard(StringPool.BLANK);
                String workerName = StringUtil.toString(host.replaceAll("\\.", ""), "_", port);
                WorkerData workerData = new Gson().fromJson(jedis.hget(ClusterDataManager.CLUSTER_WORKER_PREFIX, workerName + ClusterDataManager.WORKER_DATA_POSTFIX), WorkerData.class);
                if (workerData == null) {
                    workerData = new WorkerData();
                }
                
                Runtime rt = Runtime.getRuntime();
                String memoryUsed = readableFileSize(rt.totalMemory() - rt.freeMemory());
                String memoryLimit = readableFileSize(rt.maxMemory());
                workerData.setMemory_usage(memoryUsed);
                workerData.setMemory_limit(memoryLimit);
                
                jedis.hset(ClusterDataManager.CLUSTER_WORKER_PREFIX, workerName + ClusterDataManager.WORKER_DATA_POSTFIX, new Gson().toJson(workerData));
                isCommited = true;
        } catch (Exception e) {
            LogUtil.error(e);
        } finally {
            RedisCommand.freeRedisResource(jedisPool, shardedJedis, isCommited);
        }
    }
    
    public static String readableFileSize(long size) {
        if (size <= 0) {
            return "0";
        }
        final String[] units = new String[] { "B", "KB", "MB", "GB", "TB" };
        int digitGroups = (int) (Math.log10(size) / Math.log10(1024));
        return new DecimalFormat("#,##0.#").format(size / Math.pow(1024, digitGroups)) + " "
                + units[digitGroups];
    }
}
