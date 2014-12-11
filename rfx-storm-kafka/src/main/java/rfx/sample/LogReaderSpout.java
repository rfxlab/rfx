package rfx.sample;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;



public class LogReaderSpout extends BaseRichSpout {
	/**
	 * 
	 */
	private static final long serialVersionUID = 759043128309555422L;
	SpoutOutputCollector _collector;
	public static int rowCount = 0;
	static boolean isReading = true;
	
	static final Queue<String> logLineQueue = new ConcurrentLinkedQueue<String>();

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;
		
		File file = new File(conf.get("logfile").toString());
		System.out.println("logfile:" + file.getAbsolutePath());
		if(file.isFile()){
			readBigLogFile(file);
		} else {
			isReading = false;
		}	
	}

	@Override
	public void nextTuple() {	
		String record = null;		
		if ((record = logLineQueue.poll()) != null) {
			//System.out.println("nextTuple: " + record);
			_collector.emit(new Values(record));
		} else {
			Utils.sleep(1000);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("url"));
	}
	

	public boolean hasNext() {
		if(logLineQueue.isEmpty() && ! isReading ){
			return false;
		}
		return true;
	}
	

	public int pushLogLinesToQueue(InputStreamReader in) {
		BufferedReader br = new BufferedReader(in);
		int c = 0;
		try {
			int linesPerRead = 2000;
			for (int i = 0; i < linesPerRead; ++i) {
				String sline = br.readLine();
				if (sline == null)
					break;
				logLineQueue.add(sline);
				c++;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return c;
	}

	public void readBigLogFile(final File file) {
		try {
			final InputStreamReader reader = new InputStreamReader(new FileInputStream(file), "UTF-8");
			if ( ! reader.ready() ) {
				isReading = false;
			}			
			
			new Thread(new Runnable() {				
				@Override
				public void run() {								
					try {
						int c = pushLogLinesToQueue(reader);
						rowCount += c;
						while (c > 0) {
							c = pushLogLinesToQueue(reader);
							rowCount += c;
							Thread.sleep(50);
						}
						reader.close();
						isReading = false;
					} catch (Exception e) {						
						e.printStackTrace();
					} finally {
						try {
							reader.close();
						} catch (IOException e) {}
					}
				}
			}).start();			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
