package rfx.core.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Queue;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;


/**
 * write file using buffer and timer, asynchronous write file (storm logs, raw logs)
 * 
 * @author trieu
 *
 */
public class AsynFileWriter {

	long expiredTime = 960000L;//15 minutes
	long timeToFlush = 10000;	
	Timer timer = new Timer(true);
	ConcurrentMap<String, LogData> fileMap = new ConcurrentHashMap<>();
		
	public AsynFileWriter(long expiredTime, long timeToFlush) {
		super();
		this.expiredTime = expiredTime;
		this.timeToFlush = timeToFlush;
		startTimer();		
	}
	
	public AsynFileWriter(long timeToFlush) {
		super();
		this.timeToFlush = timeToFlush;
		startTimer();		
	}
	
	public AsynFileWriter() {
		super();
		startTimer();		
	}
	
	public void shutdownTimer(){
		try {
			flushDataToFile();
			timer.cancel();
		} catch (Exception e) {}
	}
	
	public void flushDataToFile(){
		try {
			Set<String> filePaths = fileMap.keySet();
			for (String filePath : filePaths) {
				LogData rawLogData = fileMap.get(filePath);
				if(rawLogData.size() == 0){
					File file = new File(filePath);
					if(file.isFile()){
						long diff = System.currentTimeMillis() - file.lastModified();
						if(diff > AsynFileWriter.this.expiredTime){
							fileMap.remove(filePath);		
						}
					}						
				} else {
					rawLogData.writeToFile();
				}
			}
		} catch (Exception e) {
			LogUtil.e("AsynFileWriter.flushDataToFile", e.getMessage());
		}
	}
	
	void startTimer(){		
		timer.schedule(new TimerTask() {			
			@Override
			public void run() {
				flushDataToFile();
			}
		}, 1000, this.timeToFlush);
	}

	public void write(String logFile, String data){
		LogData rawLogData = fileMap.get(logFile);
		if(rawLogData == null){			
			rawLogData = new LogData(logFile);
			fileMap.put(logFile, rawLogData);
		}
		rawLogData.addToQueue(data);	
	}
	
	public static class LogData {
		static final int BUFFER_SIZE = 1024 * 200;// 200 KB
		static final int MAX_QUEUE_SIZE = 600;

		String logFile;
		Queue<String> logQueue = new ConcurrentLinkedQueue<>();

		public LogData(String logFile) {
			super();
			this.logFile = logFile;
		}

		public String getLogFile() {
			return logFile;
		}

		public void setLogFile(String logFile) {
			this.logFile = logFile;
		}
		
		public int addToQueue(String log){
			logQueue.add(log);
			int size = logQueue.size();
			if(size>MAX_QUEUE_SIZE){
				this.writeToFile();
			}
			return size;
		}
		
		public int size(){
			return logQueue.size();
		}

		public void writeToFile() {			
			FileWriter fileWritter = null;
			BufferedWriter bufferedWriter = null;			
			try {
				File file = new File(logFile);
				if (!file.exists()) {
					file.createNewFile();
				}
				fileWritter = new FileWriter(file, true);
				bufferedWriter = new BufferedWriter(fileWritter, BUFFER_SIZE);				
				while (! logQueue.isEmpty() ) {
					String log = logQueue.poll();
					log = logQueue.poll();
					if(log == null){
						break;
					}
					bufferedWriter.write(log);	
				}
			} catch (Exception e) {			
				LogUtil.e("RawLogData", e.getMessage() + " "+logFile);
			} finally {
				if (bufferedWriter != null) {
					try {
						bufferedWriter.flush();
						bufferedWriter.close();
					} catch (IOException e) {}
				}
				if (fileWritter != null) {
					try {
						fileWritter.flush();
						fileWritter.close();
					} catch (IOException e) {}			
				}
			}
		}
	}
}