package rfx.query;


public class FunctorGroupResultByKey extends Functor {
	QueryContext queryContext;
	public FunctorGroupResultByKey(QueryContext queryContext) {
		super();
		this.queryContext = queryContext;
	}
	
	@Override
	public void onReceive(Object message) throws Exception {		
		if(message == null){
			return;
		}
		if(message instanceof Integer){
			Integer actorId = (Integer)message;				
			queryContext.collectResult(actorId);
		} 
		else if(message instanceof ActorData){
			ActorData actorData = (ActorData)message;
			queryContext.collectResult(actorData.getActorId(), actorData);				
		}
		else if(message instanceof QueryMessage){
			QueryMessage queryMessage = (QueryMessage)message;				
			queryContext.ask(queryMessage.getId(), queryMessage, self());
		} 			
		else {
			System.out.println(" unhandled message: " + message );
		}			
	}
}
