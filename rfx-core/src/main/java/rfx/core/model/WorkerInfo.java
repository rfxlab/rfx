package rfx.core.model;

import rfx.core.util.StringUtil;

import com.google.gson.Gson;

public class WorkerInfo {
	String host;
	int port = -1;
	String name;
	String topology;
	boolean alive;
	private String mainClass;

	public WorkerInfo(String host, int port, String name, boolean alive) {
		super();
		this.host = host;
		this.port = port;
		this.name = name;
		this.alive = alive;
	}

	public WorkerInfo(String name, String host, int port) {
		super();
		this.name = name;
		this.host = host;
		this.port = port;
		this.alive = false;
	}

    public WorkerInfo(String name, String host, int port, String mainClass) {
        super();
        this.name = name;
        this.host = host;
        this.port = port;
        this.mainClass = mainClass;
        this.alive = false;
    }
	
	public WorkerInfo(String host, int port, String topology) {
		super();
		this.host = host;
		this.port = port;
		this.topology = topology;
	}

    public WorkerInfo(String host, int port, String topology, String mainClass) {
        super();
        this.host = host;
        this.port = port;
        this.topology = topology;
        this.mainClass = mainClass;
    }

	public String toJson() {
		return new Gson().toJson(this);
	}
	
	public static WorkerInfo fromJson(String json) {
		return new Gson().fromJson(json, WorkerInfo.class);
	}
	
	@Override
	public String toString() {
		if(StringUtil.isEmpty(name)){
			return StringUtil.toString("worker-",this.host,":", this.port);
		}
		return name;
	}
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getHost() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}

	public boolean isAlive() {
		return alive;
	}

	public void setAlive(boolean alive) {
		this.alive = alive;
	}

    public String getMainClass() {
        return mainClass;
    }

    public void setMainClass(String mainClass) {
        this.mainClass = mainClass;
    }	
}
