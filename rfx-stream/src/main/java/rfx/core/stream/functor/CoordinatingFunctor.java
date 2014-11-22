package rfx.core.stream.functor;

import rfx.core.stream.functor.handler.BaseCoordinatingAction;
import rfx.core.stream.message.WorkerPayload;
import akka.actor.ActorRef;
import akka.actor.UntypedActor;

/**
 * Coordinator for worker, routing the WorkerPayload message to specific worker's handler
 * 
 * @author trieu
 *
 * @param <T>
 */
public class CoordinatingFunctor<T> extends UntypedActor {
	
	BaseCoordinatingAction<T> action;
	ActorRef listener;
	protected CoordinatingFunctor(BaseCoordinatingAction<T> action) {
		super();
		this.action = action;
	}
	
	@Override
	public void onReceive(Object message) {
		//System.out.println("onReceive: "+message + " getClass:"+message.getClass().getName());
		if (message instanceof WorkerPayload) {
			@SuppressWarnings("unchecked")
			WorkerPayload<T> dataPayload = (WorkerPayload<T>)message;	
			//System.out.println("dataPayload.onReceive: "+dataPayload);
			if(action != null){		
				//System.out.println("action.dataPayload.onReceive: "+dataPayload);
				action.action(dataPayload,self(),getSender());				
			}			
		} else {
			System.err.println(message + " is not instanceof DataPayload!");
		}				
	}
	
	
}