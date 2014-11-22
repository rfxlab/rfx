package rfx.query;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

public class QueryContext {
	ActorSystem actorSystem;
	QueryResult queryResult;
	Map<Integer,ActorRef> dataActors;
	Class<? extends Functor> functorClass;
	Map<Integer, QueryMessage> queryMessages = new ConcurrentHashMap<>();
	int queryActorPoolSize;
	
	//static Map<Integer, Person> personMap = new ConcurrentHashMap<>();
	
	public QueryContext(ActorSystem actorSystem, QueryResult queryResult,
			Map<Integer, ActorRef> dataActors,
			Class<? extends Functor> functorClass,int queryActorPoolSize) {
		super();
		this.actorSystem = actorSystem;
		this.queryResult = queryResult;
		this.dataActors = dataActors;
		this.functorClass = functorClass;
		this.queryActorPoolSize = queryActorPoolSize;
	}
	public QueryResult getQueryResult() {
		return queryResult;
	}
	public Map<Integer, ActorRef> getDataActors() {
		return dataActors;
	}		
	public void ask(int id, QueryMessage queryMessage, ActorRef asker){
		queryMessages.put(id, queryMessage);
		//System.out.println("ask " + id);
		ActorRef actorRef = dataActors.get(id);
		if(actorRef != null){				
			actorRef.tell(queryMessage.getQuery(), asker);
		}
	}
	public QueryMessage collectResult(int id){			
		return collectResult(id, null);
	}	
	public QueryMessage collectResult(int id, ActorData actorData){			
		//System.out.println("collectResult " + id);
		QueryMessage q = queryMessages.remove(id);
		if(actorData != null){
			String groupKey = queryResult.getFunction().apply(actorData);		
			queryResult.storeResult(actorData, groupKey);
		}
		return q;
	}
	public boolean isQueryDone(){
		return queryMessages.isEmpty() == true;
	}
	public int getQueryMessageSize(){
		return queryMessages.size();
	}
	public Class<? extends Functor> getFunctorClass() {
		return functorClass;
	}
	public ActorSystem getActorSystem() {
		return actorSystem;
	}
	public int getQueryActorPoolSize() {
		return queryActorPoolSize;
	}
	
}