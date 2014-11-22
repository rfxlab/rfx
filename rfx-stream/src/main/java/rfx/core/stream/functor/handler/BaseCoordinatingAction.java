package rfx.core.stream.functor.handler;

import rfx.core.stream.message.WorkerPayload;
import akka.actor.ActorRef;

public abstract class BaseCoordinatingAction<T> {
	protected ActorRef sender;
	protected ActorRef self;
	
	public ActorRef getSender() {
		return sender;
	}
	public void setSender(ActorRef sender) {
		this.sender = sender;
	}
	public void setSelf(ActorRef myself) {
		this.self = myself;
	}
	public ActorRef getSelf() {
		return self;
	}
	public boolean responseToSender(String message){
		if(sender != null){
			if(self != null){
				sender.tell(message, self);
			} else {
				sender.tell(message, ActorRef.noSender());
			}
			return true;
		}
		return false;
	}
	
	public abstract Object action(WorkerPayload<T> dataPayload, ActorRef self, ActorRef sender);	
}
