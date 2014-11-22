package rfx.core.stream.processor;

import java.util.ArrayList;
import java.util.List;

import rfx.core.stream.message.Fields;
import rfx.core.stream.message.Tuple;

public abstract class Processor {
	
	public Tuple processToTuple(Tuple inputTuple, Fields outputMetadataFields){
		return null;
	}
	public boolean processTuple(Tuple inputTuple){
		return false;
	}
	public List<Tuple> processToTuples(Tuple inputTuple, Fields outputMetadataFields){
		return new ArrayList<>(0);
	}
	
	public static Tuple processTuple(Class clazz, Tuple inputTuple, Fields outputMetadataFields){
		try {
			Processor processor = (Processor) clazz.newInstance();
			return processor.processToTuple(inputTuple, outputMetadataFields);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public static boolean processTuple(Class clazz, Tuple inputTuple){
		try {
			Processor processor = (Processor) clazz.newInstance();
			return processor.processTuple(inputTuple);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
}
