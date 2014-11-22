package rfx.core.stream.listener;

import java.io.Serializable;

public abstract class SimpleEventHandler implements Serializable{
	private static final long serialVersionUID = 958904344386501430L;
	boolean handledEventDone = false;
	public abstract void handle();
	
	public boolean isHandledEventDone() {
		return handledEventDone;
	}
	public void setHandledEventDone(boolean handledEventDone) {
		this.handledEventDone = handledEventDone;
	}	
	
}