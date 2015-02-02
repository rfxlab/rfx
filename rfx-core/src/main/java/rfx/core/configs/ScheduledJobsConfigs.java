package rfx.core.configs;

import java.io.IOException;
import java.util.Collection;

import rfx.core.util.FileUtils;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;


public class ScheduledJobsConfigs {
	public static final String SCHEDULED_JOBS_CONFIG_FILE = "configs/scheduled-jobs-configs.json";
	static Collection<ScheduledJobsConfigs> scheduledJobsConfigs;
	
	private String classpath;
	private long delay;
	private long period;
	private int hoursGoBack = 2;
	
	public ScheduledJobsConfigs() {}	
	
	public  String getClasspath() {
		return classpath;
	}

	public void setClasspath(String classpath) {
		this.classpath = classpath;
	}

	public long getDelay() {
		return delay;
	}

	public void setDelay(long delay) {
		this.delay = delay;
	}
	
	public long getPeriod() {
		return period;
	}

	public void setPeriod(long period) {
		this.period = period;
	}
	
	public int getHoursGoBack() {
		return hoursGoBack;
	}
	
	public void setHoursGoBack(int hoursGoBack) {
		this.hoursGoBack = hoursGoBack;
	}

	@Override
	public String toString() {
		StringBuilder s = new StringBuilder();
		s.append("classpath:").append(classpath).append("-");
		s.append("delay:").append(delay).append("-");
		s.append("period:").append(period);		
		return s.toString();
	}

	
	public static Collection<ScheduledJobsConfigs> load(){
		if(scheduledJobsConfigs == null){
			scheduledJobsConfigs = loadFromFile(SCHEDULED_JOBS_CONFIG_FILE);
		}
		return scheduledJobsConfigs;
	}

	public static Collection<ScheduledJobsConfigs> loadFromFile(String filePath){		
		try {
			String json = FileUtils.readFileAsString(filePath);			
			java.lang.reflect.Type collectionType = new TypeToken<Collection<ScheduledJobsConfigs>>(){}.getType();
			Collection<ScheduledJobsConfigs> ints2 = new Gson().fromJson(json, collectionType);						
			return ints2;
		} catch (IOException e) {
			e.printStackTrace();		
		}
		throw new IllegalArgumentException("Can not load "+SCHEDULED_JOBS_CONFIG_FILE);
	}
}