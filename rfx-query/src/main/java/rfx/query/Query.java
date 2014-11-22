package rfx.query;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

import rfx.core.util.Utils;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

public class Query<T> {
	String description;
	PredicateFactory predicateFactory;			
	Map<Integer, ActorRef> queryActorPool;
	QueryContext queryContext;
	int lastIndexActor;
	
	public Query( QueryContext queryContext,String description,PredicateFactory predicateFactory) {
		super();
		this.queryContext = queryContext;
		this.description = description;
		this.predicateFactory = predicateFactory;
		this.queryActorPool = createQueryActorPool(queryContext);
		lastIndexActor = queryContext.getQueryActorPoolSize() - 1;
	}
	
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}	
	
	public ActorRef getQueryActor() {			
		ActorRef queryActor = queryActorPool.get(Utils.randomNumber(0, lastIndexActor));
		return queryActor;
	}
	
	public void execute(){
		 queryContext.getDataActors().keySet().forEach((Integer id)->{			
			Predicate<?> predicate = predicateFactory.build();
			this.getQueryActor().tell(new QueryMessage(predicate , id),null);			
		});
	}
	
	public static Map<Integer, ActorRef> createQueryActorPool( QueryContext queryContext){
		ActorSystem actorSystem = queryContext.getActorSystem();
		int poolSize = queryContext.getQueryActorPoolSize();
		Map<Integer, ActorRef> queryActorPool = new HashMap<>(poolSize);
		for (int i = 0; i < poolSize; i++) {
			ActorRef queryActor = actorSystem.actorOf(Props.create(queryContext.getFunctorClass(), queryContext));
			queryActorPool.put(i, queryActor);
		}
		return queryActorPool;
	}
}
