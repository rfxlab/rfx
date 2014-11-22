package rfx.core.model;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;

public class WorkerStatusInfo {

	public final static int WORKER_DIED = 0;
	public final static int WORKER_HEALING = 1;
	public final static int WORKER_ALIVE = 2;
	
	int status = WORKER_DIED;
	long processedLogs = 0;
	List<String> topics = new ArrayList<>();
	String ip;
	int port;
	String workerId;
	int usedMemory;
	int maxHeadMemory;	
	long updatedtime;
	
	public WorkerStatusInfo(String workerId, int status) {
		super();
		this.status = status;
		this.workerId = workerId;
	}
	
	public int getStatus() {
		return status;
	}
	public void setStatus(int status) {
		this.status = status;
	}
	public long getProcessedLogs() {
		return processedLogs;
	}
	public void setProcessedLogs(long processedLogs) {
		this.processedLogs = processedLogs;
	}
	public List<String> getTopics() {
		return topics;
	}
	public void setTopics(List<String> topics) {
		this.topics = topics;
	}
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
	public String getWorkerId() {
		return workerId;
	}
	public void setWorkerId(String workerId) {
		this.workerId = workerId;
	}
	public int getUsedMemory() {
		return usedMemory;
	}
	public void setUsedMemory(int usedMemory) {
		this.usedMemory = usedMemory;
	}
	public int getMaxHeadMemory() {
		return maxHeadMemory;
	}
	public void setMaxHeadMemory(int maxHeadMemory) {
		this.maxHeadMemory = maxHeadMemory;
	}
	
	
	
	public long getUpdatedtime() {
		return updatedtime;
	}

	public void setUpdatedtime(long updatedtime) {
		this.updatedtime = updatedtime;
	}

	@Override
	public String toString() {
		return new Gson().toJson(this);
	}
	
	public static WorkerStatusInfo fromJson(String json) {
		return new Gson().fromJson(json, WorkerStatusInfo.class);
	}
	
}
