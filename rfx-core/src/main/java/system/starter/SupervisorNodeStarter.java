package system.starter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.util.Properties;

import rfx.core.configs.loader.ConfigAutoLoader;
import rfx.core.stream.node.SupervisorNode;
import rfx.core.util.LogUtil;
import rfx.core.util.Utils;

//http://steveliles.github.io/invoking_processes_from_java.html
public class SupervisorNodeStarter {

    static int startWorker(String workerJar, String mainClass) {
        int pid = 0;
        try {
            String java_path = "";
            Properties prop = new Properties();
            File commonPropFile = new File("build.properties");
            if (commonPropFile.isFile()) {
                prop.load(new FileInputStream(commonPropFile));
                java_path = prop.getProperty("java_path");
            }
            ProcessBuilder pb = new ProcessBuilder(java_path, "-cp", "lib/*:" + workerJar,
                    mainClass);
            Process p = pb.start();
            Field f = p.getClass().getDeclaredField("pid");
            f.setAccessible(true);
            pid = f.getInt(p);
        } catch (Throwable e) {
            e.printStackTrace();
        }
        return pid;
    }

    public static String execCmd(String cmd, String filter) {
        StringBuilder output = new StringBuilder();
        Process p = null;
        try {
            p = Runtime.getRuntime().exec(cmd);
            BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line;
            while ((line = input.readLine()) != null) {
                if (!filter.isEmpty()) {
                    if (line.contains(filter)) {
                        output.append(line).append("\n");
                    }
                } else {
                    output.append(line).append("\n");
                }
            }
            input.close();
        } catch (Exception err) {
            err.printStackTrace();
        } finally {
            if (p != null) {
                p.destroy();
            }
        }
        return output.toString();
    }

    public static void main(String[] args) throws Exception {
        ConfigAutoLoader.loadAll();
        boolean autoHeal = true;
        if (args.length > 0) {
            autoHeal = args[0].equalsIgnoreCase("autoheal:true");
        }
        String nodeId = "supervisor-node";
        LogUtil.setSuffixLogFile(nodeId);
        LogUtil.setLogFileHourly(false);
        System.out.println("------autoheal:" + autoHeal);
        SupervisorNode.start(autoHeal);

        Utils.sleep(2000);
        Utils.foreverLoop();
    }
}
