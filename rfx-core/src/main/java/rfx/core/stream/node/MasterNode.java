package rfx.core.stream.node;

import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.VertxFactory;
import org.vertx.java.core.http.HttpServerRequest;

import rfx.core.model.Callback;
import rfx.core.model.CallbackResult;
import rfx.core.model.HttpServerRequestCallback;
import rfx.core.model.WorkerInfo;
import rfx.core.stream.cluster.ClusterDataManager;
import rfx.core.stream.node.handler.HttpHandlerMapper;
import rfx.core.stream.node.handler.MasterNodeHttpHandlers;
import rfx.core.util.StringPool;
import rfx.core.util.Utils;

public class MasterNode {

    String masterHostname;
    int masterHttpPort;
    int masterWebSocketPort;
    static final Timer theTimer = new Timer(true);
    static long timeToMonitoringWorkers = 10000;// 10 seconds
    final static String BASE_RESOURCE = "./resources";
    final static String FAVICON_PATH = BASE_RESOURCE + "/img/favicon.ico";
    final static String DASHBOARD_PATH = BASE_RESOURCE + "/html/index.html";

    public MasterNode(String masterHostname, int masterHttpPort, int masterWebSocketPort) {
        this.masterHostname = masterHostname;
        this.masterHttpPort = masterHttpPort;
        this.masterWebSocketPort = masterWebSocketPort;
    }

    public void run() {
        try {
            final HttpHandlerMapper mapper = HttpHandlerMapper.get(MasterNodeHttpHandlers.class);
            Vertx vertx = VertxFactory.newVertx();
            vertx.createHttpServer().requestHandler(new Handler<HttpServerRequest>() {

                public void handle(HttpServerRequest request) {

                    String contentType = StringPool.MediaType.HTML;
                    String path = request.path();
                    boolean isPost = false;
                    if ("POST".equals(request.method().toUpperCase())) {
                        request.expectMultiPart(true);
                        isPost = true;
                    }

                    if (path.equals("/favicon.ico")) {
                        request.response().sendFile(FAVICON_PATH);
                        return;
                    } else if (path.equals("/") || path.equals("")) {
                        request.response().sendFile(DASHBOARD_PATH);
                        return;
                    } else if (path.startsWith("/img/") || path.startsWith("/js/")) {
                        request.response().sendFile(BASE_RESOURCE + path);
                        return;
                    } else if (path.endsWith(".html") || path.endsWith(".css")
                            || path.endsWith(".js")) {
                        request.response().sendFile(BASE_RESOURCE + path);
                        return;
                    }

                    request.response().putHeader("Content-Type", contentType);
                    HttpServerRequestCallback callback = mapper.getByPath(path);
                    if (callback != null) {
                        if (isPost) {
                            callback.handleHttpPostRequest(request);
                            return;
                        } else {
                            CallbackResult<String> rs = callback.handleHttpGetRequest(request);
                            if (rs != null) {
                                request.response().end(rs.getResult());
                            } else {
                                request.response().end("Got Null data from path " + path);
                            }
                        }
                    } else {
                        String err = "No HTTP handler found for the path: " + path;
                        System.err.println(err);
                        request.response().end(err);
                    }
                }
            }).listen(masterHttpPort, masterHostname);

        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        } finally {
            System.out.println("MasterNode.started OK at host:" + masterHostname + " http-port:"
                    + masterHttpPort + " ws-port:" + masterWebSocketPort);
            System.out.println("####### begin monitor all workers ########");
            monitorAllWorkersAndUpdateStatus();
        }

        Utils.foreverLoop(1000, new Callback<Boolean>() {

            @Override
            public CallbackResult<Boolean> call() {
                return new CallbackResult<Boolean>(false);
            }
        });
    }

    /**
     * for master only
     */
    private void monitorAllWorkersAndUpdateStatus() {
        theTimer.schedule(new TimerTask() {

            @Override
            public void run() {
                Map<String, WorkerInfo> workers = ClusterDataManager.getWorkerInfoFromRedis();
                Set<String> workerNames = workers.keySet();
                for (String workerName : workerNames) {
                    WorkerInfo node = workers.get(workerName);
                    try {
                        if (node != null) {
                            boolean isAlive = ClusterDataManager.ping(node);
                            node.setAlive(isAlive);
                            ClusterDataManager.saveWorkerInfo(node);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }, 5000, timeToMonitoringWorkers);
    }
}
