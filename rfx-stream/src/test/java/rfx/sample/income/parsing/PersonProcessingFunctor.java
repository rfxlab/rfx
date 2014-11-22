package rfx.sample.income.parsing;

import rfx.core.stream.functor.BaseFunctor;
import rfx.core.stream.message.Tuple;
import rfx.core.stream.model.DataFlowInfo;
import rfx.core.stream.topology.BaseTopology;
import rfx.core.util.LogUtil;

public class PersonProcessingFunctor extends BaseFunctor  {
	
	protected PersonProcessingFunctor(DataFlowInfo dataFlowInfo, BaseTopology topology) {
		super(dataFlowInfo, topology);
	}

	@Override
	public void onReceive(Object message) throws Exception {
		if (message instanceof Tuple) {			
			Tuple inputTuple = (Tuple) message;
			try {
				Person person = (Person) inputTuple.getValueByField("person");
				System.out.println("person# "+person);
		
			} catch (Exception e) {
				LogUtil.error(e);
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
