package rfx.core.job;


import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;

import rfx.core.configs.ScheduledJobsConfigs;



public class ScheduledJobManager {
	
	private static Collection<ScheduledJobsConfigs> scheduledJobsConfigs;
	
	Map<String,ScheduledJob> jobs = new HashMap<>();
	static ScheduledJobManager _instance;
	static Timer scheduledService = new Timer(true);//daemon process
	
	public static ScheduledJobManager getInstance(){
		if(_instance == null){
			_instance = new ScheduledJobManager();
		}
		return _instance;
	}
	
	protected ScheduledJobManager() {
		if(scheduledJobsConfigs == null){
			scheduledJobsConfigs = ScheduledJobsConfigs.load();	
			int size = scheduledJobsConfigs.size();
			System.out.println("AutoTasksScheduler started with "+ size + " tasks in queue");
			
		}
	}
	
	public boolean triggerJob(String classpath){
		final ScheduledJob autoTask = jobs.get(classpath);
		if(autoTask != null){
			new Thread(new Runnable() {				
				@Override
				public void run() {
					autoTask.run();		
				}
			}).start();			
			return true;
		}
		return false;
	}
	
	public int startScheduledJobs(){
		for (ScheduledJobsConfigs config : scheduledJobsConfigs) {
			if(config.getDelay() >= 0){
				try {
					String classpath = config.getClasspath();
					System.out.println("#process autoTaskConfig:"+config + " "+classpath);
					
					Class<?> clazz = Class.forName(classpath);
					ScheduledJob autoTask = (ScheduledJob) clazz.newInstance();
					autoTask.setScheduledJobsConfigs(config);
					
					if(config.getPeriod() == 0){
						scheduledService.schedule(autoTask, config.getDelay()*1000L);
					} else {
						scheduledService.schedule(autoTask, config.getDelay()*1000L, config.getPeriod()*1000L);	
					}
					jobs.put(classpath,autoTask);			
				}  catch (Exception e) {				
					e.printStackTrace();
				}
			}
		}
		return jobs.size();
	}
}

