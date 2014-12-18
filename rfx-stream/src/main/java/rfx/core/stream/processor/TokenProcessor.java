package rfx.core.stream.processor;

import rfx.core.stream.functor.common.DataSourceFunctor;
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

	public static final String PARTITION_ID = "partitionId";
	public static final String TOPIC = "topic";
	public static final String USERAGENT = "useragent";
	public static final String IP = "ip";
	public static final String LOGGEDTIME = "loggedtime";
	public static final String COOKIE = "cookie";
	public static final String QUERY = "query";
	public static final String _EMPTY = "_empty";
	public static final String _FROM_FILE = "_from_file";
	public static final String TAB_STRING = "\t";

	@Override
	public Tuple processToTuple(Tuple inputTuple, Fields outFields) {
		String logRow = inputTuple.getStringByField(DataSourceFunctor.EVENT);
    	String topic = inputTuple.getStringByField(TOPIC,_FROM_FILE);
    	String partitionId = inputTuple.getStringByField(PARTITION_ID,_EMPTY);	    	
		String[] logTokens = logRow.split(TAB_STRING);
		if(logTokens.length == 5){
			if(StringUtil.isNotEmpty(logTokens[0])
					&& StringUtil.isNotEmpty(logTokens[1]) 
					&& StringUtil.isNotEmpty(logTokens[2]) 
					&& StringUtil.isNotEmpty(logTokens[3]) 
					&& StringUtil.isNotEmpty(logTokens[4])){
				String ip = StringUtil.safeSplitAndGet(logTokens[0], StringPool.COLON, 0);
				int loggedtime = StringUtil.safeParseInt(logTokens[1]);
				String useragent = StringUtil.safeString(logTokens[2]);
				String query = logTokens[3];
				String cookie = StringUtil.safeString(logTokens[4]);			
				return new Tuple(outFields, new Values(query, cookie, loggedtime, ip, useragent, topic, partitionId));
			}
		}	
		return null;
	}
}
