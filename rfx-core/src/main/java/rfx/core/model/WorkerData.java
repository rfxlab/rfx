package rfx.core.model;

public class WorkerData {
    
    private String worker_name;
	private String hostname;
	private String status;
	private String memory_usage;
	private String memory_limit;
	private String topology_name;
	private String kafka_topic;
	private String partition;
	private String kafka_brokers;
	private String data_emitting_counter;
	private String uptime;
	private String actor_list;
	private String total_actors;

	public WorkerData(String memory_usage, String memory_limit, String worker_name, String hostname , String topology_name, String kafka_topic,
			String partition, String kafka_brokers, String data_emitting_counter, String uptime, String actor_list,
			String total_actors) {
		super();
		this.worker_name = worker_name;
		this.hostname = hostname;
		this.memory_usage = memory_usage;
		this.memory_limit = memory_limit;
		this.topology_name = topology_name;
		this.kafka_topic = kafka_topic;
		this.partition = partition;
		this.kafka_brokers = kafka_brokers;
		this.data_emitting_counter = data_emitting_counter;
		this.uptime = uptime;
		this.actor_list = actor_list;
		this.total_actors = total_actors;
	}
	

	/**
     * @param worker_name
     * @param hostname
     * @param status
     * @param memory_usage
     * @param memory_limit
     * @param uptime
     */
    public WorkerData(String worker_name, String hostname, String memory_usage,
            String memory_limit, String uptime) {
        super();
        this.worker_name = worker_name;
        this.hostname = hostname;
        this.memory_usage = memory_usage;
        this.memory_limit = memory_limit;
        this.uptime = uptime;
    }


    public WorkerData() {
		// TODO Auto-generated constructor stub
	}

	public String getWorkername() {
		return worker_name;
	}

	public void setWorkername(String worker_name) {
		this.worker_name = worker_name;
	}
	
	public String getHostname() {
		return hostname;
	}

	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getMemory_limit() {
		return memory_limit;
	}

	public void setMemory_limit(String memory_limit) {
		this.memory_limit = memory_limit;
	}

	public String getMemory_usage() {
		return memory_usage;
	}

	public void setMemory_usage(String memory_usage) {
		this.memory_usage = memory_usage;
	}

	public String getTopology_name() {
		return topology_name;
	}

	public void setTopology_name(String topology_name) {
		this.topology_name = topology_name;
	}

	public String getKafka_topic() {
		return kafka_topic;
	}

	public void setKafka_topic(String kafka_topic) {
		this.kafka_topic = kafka_topic;
	}

	public String getPartition() {
		return partition;
	}

	public void setPartition(String partition) {
		this.partition = partition;
	}

	public String getKafka_brokers() {
		return kafka_brokers;
	}

	public void setKafka_brokers(String kafka_brokers) {
		this.kafka_brokers = kafka_brokers;
	}

	public String getData_emitting_counter() {
		return data_emitting_counter;
	}

	public void setData_emitting_counter(String data_emitting_counter) {
		this.data_emitting_counter = data_emitting_counter;
	}

	public String getUptime() {
		return uptime;
	}

	public void setUptime(String uptime) {
		this.uptime = uptime;
	}

	public String getActor_list() {
		return actor_list;
	}

	public void setActor_list(String actor_list) {
		this.actor_list = actor_list;
	}

	public String getTotal_actors() {
		return total_actors;
	}

	public void setTotal_actors(String total_actors) {
		this.total_actors = total_actors;
	}
}
