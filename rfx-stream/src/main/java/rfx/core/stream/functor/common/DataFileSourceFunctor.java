package rfx.core.stream.functor.common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URL;
import java.util.concurrent.atomic.AtomicLong;

import rfx.core.model.CallbackResult;
import rfx.core.stream.message.Fields;
import rfx.core.stream.message.Tuple;
import rfx.core.stream.message.Values;
import rfx.core.stream.model.DataFlowInfo;
import rfx.core.stream.topology.BaseTopology;
import rfx.core.util.StringUtil;
import rfx.core.util.Utils;
import akka.event.Logging;
import akka.event.LoggingAdapter;

public class DataFileSourceFunctor extends DataSourceFunctor {
	
	LoggingAdapter log = Logging.getLogger(getContext().system(), this);
	
	public DataFileSourceFunctor(DataFlowInfo dataFlowInfo, BaseTopology topology) {
		super(dataFlowInfo, topology);
	}

	//what data fields that this actor would send to next actor
	static Fields fields = new Fields("row");
	
	final static String SCHEME_FILE = "file";
	final static String SCHEME_HTTP = "http";
	final static String SCHEME_FILE_PREFIX = SCHEME_FILE + "://";
	final static int BUFFER_SIZE = 5000;
	
	static AtomicLong genId = new AtomicLong();
	
	
	public static final long getCurrentCount(){
		return genId.get();
	}
	
	static int mb = 1024*1024;
	
	@Override
	public void onReceive(Object message) throws Exception {
		//System.out.println(message + " at " + System.currentTimeMillis());
		if (EMIT_LOG_EVENT.equals(message)) {
			if(isEmitting() || isStopEmitting() || isOverMaxConcurrentEmitter() ){
				//System.out.println("--------DataFileSourceFunctor.skip-------"+message);
				unhandled(message);
				return;
			}
			setEmitting(true);		
			
			CallbackResult<String> result = super.doPreProcessing();
			if(result != null){
				String uriStr = result.getResult();
				if(uriStr.startsWith("/")){
					uriStr = StringUtil.toString(SCHEME_FILE_PREFIX , uriStr);
				}
				URI uri = new URI(uriStr);				
				
				if(SCHEME_FILE.equals(uri.getScheme())){
					
				    int c = 0;				    
					File file = new File(uri);
					if( file.isFile() ){
						BufferedReader br = null;
						try {
							System.out.println(genId.incrementAndGet() + " #BEGIN DataFileSourceFunctor uri: " + uri);
							br = new BufferedReader(new FileReader(file));
							String line;
							while((line = br.readLine()) != null) {
								this.emitStringTuple(fields,line);
								if(++c % maxSizeToSleep == 0){
									Utils.sleep(500);
								}
							}
							System.out.println(" #END Total: "+c+" rows at file: " + file.getAbsolutePath());
						} catch (Exception e) {
							e.printStackTrace();
						} finally {
							if(br != null){								
								br.close();								
								//System.out.println("Wait "+timeToSleep);
								Utils.sleep(timeToSleep);
								// get Runtime instance
								Runtime instance = Runtime.getRuntime();
								// used memory
								double usedMemory =  (instance.totalMemory() - instance.freeMemory()) / mb;
								System.out.println("Used Memory: "	+ usedMemory);
								// free memory
								System.out.println("Free Memory: " + instance.freeMemory() / mb);
								// Maximum available memory
								double maxMemory = instance.maxMemory() / mb;
								System.out.println("Max Memory: " + maxMemory);
								
								double ratio = usedMemory / maxMemory;
								if(ratio > 0.6){
									Utils.sleep(5000);
								}								
								//System.out.println("Done continue ");
							}
						}
					}					
				} else if(SCHEME_HTTP.equals(uri.getScheme())){
					URL url = new URL(result.getResult());
					BufferedReader in = new BufferedReader(new InputStreamReader(url.openStream()), 2048);
			        String row = null;
			        int c = 0;
			        while (true) {
			        	row = in.readLine();
			        	if(row != null){
			        		Tuple newTuple = new Tuple(fields, new Values(row));	
							this.emit(newTuple, self());
							
							if(++c % maxSizeToSleep == 0){
								Utils.sleep(5);
							}
							super.doPostProcessing();
			        	} else {
			        		break;	
			        	}			        	
			        }			            
			        in.close();
				}				
			}
			
			setEmitting(false);
	    } else if(STOP_EMIT_LOG_EVENT.equals(message)){
	    	stopEmitting();
	    }		
		else {
	      unhandled(message);
	    }
	}	
	

}