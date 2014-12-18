package rfx.sample.income.parsing;

import java.util.concurrent.atomic.AtomicInteger;

import rfx.core.stream.functor.BaseFunctor;
import rfx.core.stream.functor.common.DataSourceFunctor;
import rfx.core.stream.message.Fields;
import rfx.core.stream.message.Tuple;
import rfx.core.stream.message.Values;
import rfx.core.stream.model.DataFlowInfo;
import rfx.core.stream.topology.BaseTopology;

public class IncomeDataParsingFunctor extends BaseFunctor  {

	static Fields schema = new Fields("person");

	protected IncomeDataParsingFunctor(DataFlowInfo dataFlowInfo, BaseTopology topology) {
		super(dataFlowInfo, topology);
	}
	
	static AtomicInteger genId = new AtomicInteger();
	
	@Override
	public void onReceive(Object message) throws Exception {
		if (message instanceof Tuple) {			
			Tuple inputTuple = (Tuple) message;
			try {				
				String row = inputTuple.getStringByField(DataSourceFunctor.EVENT);
				
				//System.out.println(row);
				String[] toks = row.split(",");
				if(toks.length == 15){
					int id = genId.incrementAndGet();
					String education = toks[3].trim();
					String income = toks[14].trim();
					String occupation = toks[6].trim();				
					Person person = new Person(id, education, occupation, income.equals("<=50K"));
					person.setRawData(row);					
					this.emit(new Tuple(schema, new Values(person)));					
				} else {
					System.err.println(" bad data row "+row);
				}		
			} catch (Exception e) {
				e.printStackTrace();				
				//log.error(ExceptionUtils.getStackTrace(e));
			} finally {
				inputTuple.clear();
			}		
			this.doPostProcessing();
		} else {
			unhandled(message);
		}
	}
}