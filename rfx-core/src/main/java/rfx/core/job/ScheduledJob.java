package rfx.core.job;

import java.util.Date;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

import rfx.core.configs.ScheduledJobsConfigs;
import rfx.core.util.DateTimeUtil;
import rfx.core.util.LogUtil;

public abstract class ScheduledJob extends TimerTask{
	
	private ScheduledJobsConfigs scheduledJobsConfigs;
	protected int hourGoBack = 2;
	protected AtomicInteger jobStatus = new AtomicInteger(-1);
	protected String name;
	
	public ScheduledJob() {		
		super();		
		name = getClass().getName();
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
		//String rsJob = DateTimeUtil.formatDateHourMinute(new Date()) +":"+saveCount;
		//ClusterInfoManager.logSynchDataJobResult(jobName + "-hourback:"+this.hourGoBack, rsJob);
		System.out.println(jobName + "-hourback:"+this.hourGoBack);
	}
	
	@Override
	public void run() {
		if (jobStatus.get() <= 0) {
			jobStatus.set(1);			
			try {
				LogUtil.i(name, " is started at "+new Date().toString(), true);
				doTheJob();
				LogUtil.i(name, " is finished at "+new Date().toString(), true);
			} catch (Throwable e) {
				LogUtil.error(e);
				LogUtil.i(name, " is crashed at "+new Date().toString(), true);
			}
			jobStatus.set(0);
		} else {
			System.out.println(getClass().getName() + " is still running and not done!");
		}
	}
	
}
