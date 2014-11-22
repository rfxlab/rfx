package rfx.sample.income;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import rfx.core.stream.topology.BaseTopology;
import rfx.core.stream.topology.Pipeline;
import rfx.core.util.Utils;
import rfx.sample.income.parsing.IncomeDataParsingFunctor;
import rfx.sample.income.parsing.PersonProcessingFunctor;


public class IncomeDataProcessingTopology extends BaseTopology  {

	@Override
	public BaseTopology buildTopology(){
		return Pipeline.create(this)				
				.apply(IncomeDataParsingFunctor.class)
				.apply(PersonProcessingFunctor.class)		
				.done();
	}	
	
	//https://docs.google.com/document/d/1nwvnF71TZ1zRZ8NtGVfB52AT3kRO5FmYQ1Ofhfjp_4I/edit?usp=sharing
	//http://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.data
	final static String SAMPLE_DATA_PATH = "file:///home/trieu/data/user-income.txt";
	public static void main(String[] args) throws IOException, URISyntaxException {		
		System.out.println(new URI(SAMPLE_DATA_PATH).getScheme());		
		BaseTopology topology = new IncomeDataProcessingTopology();
		topology.addDataFile(SAMPLE_DATA_PATH);
		//topology.addDataFile("http://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.data");
		topology.buildTopology().start(500);
		Utils.sleep(2000);
	}
	
}
