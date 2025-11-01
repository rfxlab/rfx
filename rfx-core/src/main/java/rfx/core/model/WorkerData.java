package rfx.core.model;

import java.util.Objects;

/**
 * Represents a worker process in the system and its runtime metrics.
 * 
 * Provides safe enum-based status and structured resource information.
 * @author Trieu Nguyen
 */
public class WorkerData {

    public enum Status {
        STARTING, STARTED, RUNNING, PAUSED, KILLED
    }

    private String workerName;
    private String hostname;
    private Status status = Status.STARTING;

    private String memoryUsage;
    private String memoryLimit;
    private String topology;
    private String topic;
    private String partition;
    private String brokers;
    private String emittingCounter;
    private String uptime;
    private String actorList;
    private String totalActors;

    // --- Constructors ---

    public WorkerData() {
    }

    public WorkerData(String workerName, String hostname, String memoryUsage, String memoryLimit, String uptime) {
        this.workerName = workerName;
        this.hostname = hostname;
        this.memoryUsage = memoryUsage;
        this.memoryLimit = memoryLimit;
        this.uptime = uptime;
    }

    public WorkerData(String memoryUsage, String memoryLimit, String workerName, String hostname,
                      String topology, String topic, String partition, String brokers,
                      String emittingCounter, String uptime, String actorList, String totalActors) {
        this(workerName, hostname, memoryUsage, memoryLimit, uptime);
        this.topology = topology;
        this.topic = topic;
        this.partition = partition;
        this.brokers = brokers;
        this.emittingCounter = emittingCounter;
        this.actorList = actorList;
        this.totalActors = totalActors;
    }

    // --- Getters & Setters ---

    public String getWorkerName() { return workerName; }
    public void setWorkerName(String workerName) { this.workerName = workerName; }

    public String getHostname() { return hostname; }
    public void setHostname(String hostname) { this.hostname = hostname; }

    public Status getStatus() { return status; }
    public void setStatus(Status status) { this.status = status; }

    public String getMemoryUsage() { return memoryUsage; }
    public void setMemoryUsage(String memoryUsage) { this.memoryUsage = memoryUsage; }

    public String getMemoryLimit() { return memoryLimit; }
    public void setMemoryLimit(String memoryLimit) { this.memoryLimit = memoryLimit; }

    public String getTopology() { return topology; }
    public void setTopology(String topology) { this.topology = topology; }

    public String getTopic() { return topic; }
    public void setTopic(String topic) { this.topic = topic; }

    public String getPartition() { return partition; }
    public void setPartition(String partition) { this.partition = partition; }

    public String getBrokers() { return brokers; }
    public void setBrokers(String brokers) { this.brokers = brokers; }

    public String getEmittingCounter() { return emittingCounter; }
    public void setEmittingCounter(String emittingCounter) { this.emittingCounter = emittingCounter; }

    public String getUptime() { return uptime; }
    public void setUptime(String uptime) { this.uptime = uptime; }

    public String getActorList() { return actorList; }
    public void setActorList(String actorList) { this.actorList = actorList; }

    public String getTotalActors() { return totalActors; }
    public void setTotalActors(String totalActors) { this.totalActors = totalActors; }

    // --- Utility methods ---

    @Override
    public String toString() {
        return String.format(
            "WorkerData{name=%s, host=%s, status=%s, mem=%s/%s, uptime=%s, topic=%s, partition=%s}",
            workerName, hostname, status, memoryUsage, memoryLimit, uptime, topic, partition
        );
    }

    @Override
    public int hashCode() {
        return Objects.hash(workerName, hostname);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof WorkerData)) return false;
        WorkerData other = (WorkerData) obj;
        return Objects.equals(workerName, other.workerName) &&
               Objects.equals(hostname, other.hostname);
    }
}
