package rfx.core.job;

import java.util.Date;
import java.util.TimerTask;

import rfx.core.configs.ScheduledJobsConfigs;
import rfx.core.util.DateTimeUtil;

public abstract class ScheduledJob extends TimerTask{
	
	private ScheduledJobsConfigs scheduledJobsConfigs;
	protected int hourGoBack = 2;
	protected volatile boolean isWorking;
	
	public ScheduledJob() {		
		super();		
	}
	
	public int getHourGoBack() {
		return hourGoBack;
	}

	public void setHourGoBack(int hourGoBack) {
		this.hourGoBack = hourGoBack;
	}

	public ScheduledJobsConfigs getScheduledJobsConfigs() throws IllegalArgumentException{
		if(scheduledJobsConfigs == null){
			throw new IllegalArgumentException("Missing scheduledJobsConfigs setting ");
		}
		return scheduledJobsConfigs;
	}

	public void setScheduledJobsConfigs(ScheduledJobsConfigs scheduledJobsConfigs) {
		this.scheduledJobsConfigs = scheduledJobsConfigs;
	}
	
	public abstract void doTheJob();
	
	protected void logJobResult(String jobName, int saveCount){
		String rsJob = DateTimeUtil.formatDateHourMinute(new Date()) +":"+saveCount;
		//ClusterInfoManager.logSynchDataJobResult(jobName + "-hourback:"+this.hourGoBack, rsJob);
		System.out.println(jobName + "-hourback:"+this.hourGoBack);
	}
	
	@Override
	public synchronized void run() {
		if (!isWorking) {
			isWorking = true;
			System.out.println(getClass().getName() + " is doing");
			doTheJob();
			isWorking = false;
		} else {
			System.out.println(getClass().getName() + " is still running and not done!");
		}
	}
	
}
