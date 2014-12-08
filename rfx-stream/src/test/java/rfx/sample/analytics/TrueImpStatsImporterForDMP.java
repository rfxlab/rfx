package rfx.sample.analytics;

import java.io.File;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import rfx.core.stream.emitters.EmittedDataListener;
import rfx.core.stream.functor.common.DataSourceFunctor;
import rfx.core.stream.topology.BaseTopology;
import rfx.core.stream.topology.Pipeline;
import rfx.core.util.Utils;

/**
 * @author trieunt
 *
 */
public class TrueImpStatsImporterForDMP {
	
	static class TrueImpProcessingTopology extends BaseTopology{				
		public TrueImpProcessingTopology() {
			super();		
		}		
		@Override
		public BaseTopology buildTopology(){
			return Pipeline.create(this)				
					.apply(TrueImpRawLogProcessor.class)		
					.done();
		}	
	}

	public static void main(String[] args) throws UnknownHostException {						
		//ClickStatsImporterForDMP.createUserLogIndex();
		
		List<String> files = new ArrayList<String>();		
		ClickStatsImporterForDMP.listFilesForFolder(new File("/home/trieu/data/raw_logs/true_imp/day=2014-10-26"), files);
		
		DataSourceFunctor.setMaxSizeToSleep(10000);
		DataSourceFunctor.setTimeToSleep(1500);
		DataSourceFunctor.setMaxConcurrentEmitter(6);
		
		BaseTopology topology = new TrueImpProcessingTopology();
		
		int numberFilePerEmitter = 30;
		
		Queue<String> dataFiles = new LinkedList<String>();
		for (int i = 0; i < files.size(); i++) {
			dataFiles.add(files.get(i));			
			if(dataFiles.size() >= numberFilePerEmitter){
				//full 
				topology.addDataFiles(dataFiles);	
				
				//reset as new list after addDataFiles
				dataFiles = new LinkedList<String>();
			}		
		}
		
		if(dataFiles.size() > 0){
			//the rest 
			topology.addDataFiles(dataFiles);
		}	
		System.out.println("files "+files.size() + " getDataEmitterSize = "+topology.getDataEmitterSize());
		Utils.sleep(1000);
	
		topology.setEmittedDataListener(new EmittedDataListener() {
			@Override
			public void processingDone() {
				System.out.println("Done !!!! at " + new Date());
			}
		});
		topology.buildTopology().start(2000);
		//Utils.sleep(10000);
		
		System.out.println("getCurrentRowCount "+TrueImpRawLogProcessor.getCurrentRowCount());
		
		Utils.foreverLoop();
		
	}
	
//	127.0.0.1:6379> pfcount unique-visitor
//	(integer) 829933

//	127.0.0.1:6379> pfcount unique-visitor
//	(integer) 829933	
	
}
