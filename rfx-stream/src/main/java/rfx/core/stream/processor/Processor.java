package rfx.core.stream.processor;

import rfx.core.stream.message.Fields;
import rfx.core.stream.message.Tuple;

public abstract class Processor {
	
	public Tuple process(Tuple inputTuple, Fields outputMetadataFields){
		return null;
	}
	
	public boolean process(Tuple inputTuple){
		return false;
	}
	
	public static Tuple process(Class<?> clazz, Tuple inputTuple, Fields outputMetadataFields){
		try {
			Processor processor = (Processor) clazz.newInstance();
			return processor.process(inputTuple, outputMetadataFields);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public static boolean process(Class clazz, Tuple inputTuple){
		try {
			Processor processor = (Processor) clazz.newInstance();
			return processor.process(inputTuple);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
}
