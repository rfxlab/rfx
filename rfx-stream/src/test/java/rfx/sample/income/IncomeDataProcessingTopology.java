package rfx.sample.income;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;

import rfx.core.stream.emitters.EmittedDataListener;
import rfx.core.stream.topology.BaseTopology;
import rfx.core.stream.topology.Pipeline;
import rfx.core.stream.topology.PipelineTopology;
import rfx.core.util.Utils;
import rfx.sample.income.parsing.IncomeDataParsingFunctor;
import rfx.sample.income.parsing.PersonProcessingFunctor;


public class IncomeDataProcessingTopology extends PipelineTopology  {

	@Override
	public BaseTopology build(){
		return Pipeline.create(this)				
				.apply(IncomeDataParsingFunctor.class)
				.apply(PersonProcessingFunctor.class)		
				.done();
	}	
	
	//https://docs.google.com/document/d/1nwvnF71TZ1zRZ8NtGVfB52AT3kRO5FmYQ1Ofhfjp_4I/edit?usp=sharing
	//http://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.data
	final static String SAMPLE_DATA_PATH = "file:////home/trieu/data/user-income.txt";
	public static void main(String[] args) throws IOException, URISyntaxException {
		System.out.println("Start !!!! at " + new Date());
		System.out.println(new URI(SAMPLE_DATA_PATH).getScheme());		
		final BaseTopology topology = new IncomeDataProcessingTopology();
		final AtomicBoolean done = new AtomicBoolean(false);
		final String keyEmitter = "singleFileEmitter";
		topology.setEmittedDataListener(new EmittedDataListener(10000,8000) {
			@Override
			public void processingDone() {
				if(!done.get()){					
					//do something when finish processing all data files
					System.out.println("Done !!!! at " + new Date());
					done.set(true);
				}
			}
			@Override
			public void cronJob() {
				if(done.get()){
					//seed new data files into topology
					System.out.println("addDataFile !!!! at " + new Date());
					topology.addDataFile(keyEmitter, SAMPLE_DATA_PATH);
					done.set(false);
				}
			}
		});
		
		topology.addDataFile(keyEmitter, SAMPLE_DATA_PATH);
		//topology.addDataFile("http://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.data");
		topology.buildTopology().start(500);
		Utils.sleep(2000);
	}
	
}
