package rfx.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class QueryResult {
	Map<String, List<Object>> reducedResults = new ConcurrentHashMap<>();
	FunctionFactory functionFactory;
			
	public QueryResult(FunctionFactory functionFactory) {
		super();
		this.functionFactory = functionFactory;
	}

	public void storeResult(Object data, String groupKey){
		//refer-link http://stackoverflow.com/questions/20414837/list-in-concurrenthashmap
		List<Object> groupByEduList = Collections.synchronizedList(new ArrayList<Object>());
        List<Object> oldGroupByEduList = reducedResults.putIfAbsent(groupKey, groupByEduList);
        if (oldGroupByEduList != null) {
        	groupByEduList = oldGroupByEduList;
        }
        groupByEduList.add(data);
	}
	
	public Map<String, List<Object>> getReducedResults() {
		return reducedResults;
	}
	
	public Function<ActorData, String> getFunction() {
		return functionFactory.build();
	}
}