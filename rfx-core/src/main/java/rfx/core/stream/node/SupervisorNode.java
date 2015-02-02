package rfx.core.stream.node;

import java.io.File;
import java.io.FileInputStream;
import java.lang.reflect.Field;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import rfx.core.configs.WorkerConfigs;
import rfx.core.model.WorkerInfo;
import rfx.core.stream.cluster.ClusterDataManager;
import rfx.core.util.StringUtil;

/**
 * @author trieu
 * 
 *         the supervisor for all workers in running server (every server can
 *         run multiple workers with 1 supervisor only)
 *
 */
public class SupervisorNode {

    static long uptime = 0;
    final static long period = 2000;

    static boolean started = false;
    static final long miliSecondsToSleep = 2000;
    static final Timer supervisorTimer = new Timer(true);

    static final Map<String, WorkerInfo> workerInfoCache2 = new ConcurrentHashMap<>();

    public static Map<String, WorkerInfo> getManagedWorkers() {
        if (workerInfoCache2.size() > 0) {
            return workerInfoCache2;
        }
        WorkerConfigs workerConfigs = WorkerConfigs.load();        
        List<WorkerInfo> tcpPorts = workerConfigs.getAllocatedWorkers();
        for (WorkerInfo w : tcpPorts) {
        	String host = w.getHost();
            String name = StringUtil.toString(host.replaceAll("\\.", ""), "_", w.getPort());
            workerInfoCache2.put(name, new WorkerInfo(name, host, w.getPort(), w.getMainClass()));
        }
        return workerInfoCache2;
    }

    public static void start(final boolean autoHeal) {
        if (started) {
            return;
        }
        supervisorTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                SupervisorNode.checkWorkerStatus(autoHeal);
            }
        }, 2000, miliSecondsToSleep);
        started = true;
    }

    static {
        Timer timer = new Timer("uptime");
        timer.schedule(new TimerTask() {

            @Override
            public void run() {
                uptime += period;
            }
        }, 0, period);
    }

    public static void checkWorkerStatus() {
        checkWorkerStatus(true);
    }

    public static void checkWorkerStatus(boolean autoHeal) {
        Map<String, WorkerInfo> workers = getManagedWorkers();

        Set<String> workerNames = workers.keySet();
        List<WorkerInfo> diedWorkerNodes = new ArrayList<>(workerNames.size());
        for (String workerName : workerNames) {
            WorkerInfo node = workers.get(workerName);
            try {
                if (node != null) {
                    boolean isAlive = ClusterDataManager.ping(node);
                    node.setAlive(isAlive);
                    System.out.println("worker:" + workerName + " isAlive:" + isAlive + " autoHeal:" + autoHeal);
                    ClusterDataManager.saveWorkerInfo(node);
                    if (!isAlive && autoHeal) {
                        diedWorkerNodes.add(node);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        for (WorkerInfo workerInfo : diedWorkerNodes) {
            int pid = startWorker(workerInfo, workerInfo.getMainClass());
            if (pid == 0) {
                System.out.println("Start worker failed. Auto re-try after 20 seconds");
            } else {
                System.out.println("PID: " + pid);
            }
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

    public static void scheduleUpdateWorkerInfo(final WorkerInfo workerInfo) {
        new Timer(true).schedule(new TimerTask() {

            @Override
            public void run() {
                // TODO
            }
        }, 2000, period);
    }

    static int startWorker(WorkerInfo workerInfo, String mainClass) {
        int pid = 0;
        try {
            System.out.println("Try starting worker " + workerInfo + " with main class "
                    + mainClass);
            String java_path = "";
            Properties prop = new Properties();
            File commonPropFile = new File("build.properties");
            if (commonPropFile.isFile()) {
                prop.load(new FileInputStream(commonPropFile));
                java_path = prop.getProperty("java_path");
            }
            ProcessBuilder pb = new ProcessBuilder(java_path, "-cp",
            		"lib/*:" + workerInfo.getMainClass() + ".jar", workerInfo.getMainClass(),
                    workerInfo.getHost(), workerInfo.getPort() + "");
            System.out.println("Running command: "
                    + java_path + " -cp lib/*:" + workerInfo.getMainClass()
                    + ".jar " + workerInfo.getMainClass() + " " + workerInfo.getHost() + " "
                    + workerInfo.getPort());
            Process p = pb.start();
            Field f = p.getClass().getDeclaredField("pid");
            f.setAccessible(true);
            pid = f.getInt(p);
        } catch (Throwable e) {
            e.printStackTrace();
        }
        return pid;
    }

//    public static void updateWorkerData(WorkerInfo workerInfo) {
//        Runtime rt = Runtime.getRuntime();
//        String memoryUsed = readableFileSize(rt.totalMemory() - rt.freeMemory());
//        String memoryLimit = readableFileSize(rt.maxMemory());
//
//        String hostname = workerInfo.getHost();
//        String workerName = workerInfo.getName();
//        WorkerData workerData = new WorkerData(workerName, hostname, memoryUsed, memoryLimit,
//                "");
//        ClusterDataManager.updateWorkerData(workerName, workerData);
//    }
}
