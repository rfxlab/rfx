package rfx.core.stream.processor;

import rfx.core.stream.message.Fields;
import rfx.core.stream.message.Tuple;
import rfx.core.stream.message.Values;
import rfx.core.util.StringPool;
import rfx.core.util.StringUtil;

/**
 * common processor for default Kafka log 
 * 
 * @author trieu
 *
 */
public class TokenProcessor extends Processor{

	private static final String TAB_STRING = "\t";

	@Override
	public Tuple processToTuple(Tuple inputTuple, Fields outputMetadataFields) {
		String logRow = inputTuple.getStringByField("log_row");
    	String topic = inputTuple.getStringByField("topic");
    	String partitionId = inputTuple.getStringByField("partitionId");	    	
		String[] logTokens = logRow.split(TAB_STRING);		  	
		System.out.println("###logRow:"+logRow);
		if(logTokens.length == 5){
			if(	   StringUtil.isNotEmpty(logTokens[0])
				&& StringUtil.isNotEmpty(logTokens[1]) 
				&& StringUtil.isNotEmpty(logTokens[2]) 
				&& StringUtil.isNotEmpty(logTokens[3]) 
				&& StringUtil.isNotEmpty(logTokens[4])
				){
				String ip = StringUtil.safeSplitAndGet(logTokens[0], StringPool.COLON, 0);
				int loggedtime = StringUtil.safeParseInt(logTokens[1]);
				String useragent = StringUtil.safeString(logTokens[2]);
				String query = logTokens[3];
				String cookie = StringUtil.safeString(logTokens[4]);
			
				return new Tuple(outputMetadataFields, new Values(query ,cookie ,loggedtime ,ip ,useragent ,topic ,partitionId));
			}
		}	
		return null;
	}
}
