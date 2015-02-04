package rfx.core.job;

import org.vertx.java.core.Handler;
import org.vertx.java.core.http.HttpServerRequest;

import rfx.core.stream.node.worker.BaseWorker;
import rfx.core.util.LogUtil;

public class ScheduledJobNode extends BaseWorker {

	public ScheduledJobNode(String name) {
		super(name);
	}

	@Override
	public void start(String host, int port) {
		Handler<HttpServerRequest> handler = new Handler<HttpServerRequest>() {
            public void handle(HttpServerRequest request) {
                if (request.absoluteURI().getPath().equals("/kill")) {
                    request.response().end("Exiting after 5s ...");
                    killWorker();
                    return;
                } else if (request.absoluteURI().getPath().equals("/ping")) {
                    request.response().end("PONG");
                    return;
                } else if (request.absoluteURI().getPath().equals("/job/trigger")) {
                	String jobClasspath =  request.params().get("classpath");
                	boolean ok = ScheduledJobManager.getInstance().triggerJob(jobClasspath);
                    request.response().end("triggerJob "+ jobClasspath + " : "+ok);
                    return;
                }
                request.response().end("ScheduledJobNode is running");
            }
        };
        registerWorkerHttpHandler(host, port, handler);
	}
	
	 @Override
	 protected void onStartDone() {
		LogUtil.setSuffixLogFile(getName());
		int c = ScheduledJobManager.getInstance().startScheduledJobs();
		LogUtil.i("ScheduledJobManager.started "+ c + " ScheduledJobs");
	 }

}
