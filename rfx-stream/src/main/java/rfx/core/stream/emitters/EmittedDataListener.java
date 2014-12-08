package rfx.core.stream.emitters;

import java.util.Timer;
import java.util.TimerTask;


/**
 * @author trieunt
 *
 */
public abstract class EmittedDataListener {
	
	public abstract void processingDone();
	
	public void cronJob(){
		//default is nothing
	}
	
	public EmittedDataListener() {
		
	}
	
	public EmittedDataListener(long delay, long interval) {
		new Timer(true).schedule(new TimerTask() {			
			@Override
			public void run() {
				cronJob();
			}
		}, delay, interval);
	}
}
