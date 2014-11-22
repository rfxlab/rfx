package rfx.query;

import java.io.Serializable;

public class ActorData implements Serializable {			
	private static final long serialVersionUID = 6250370648916682454L;
	int actorId;		
	public ActorData(int actorId) {
		super();
		this.actorId = actorId;
	}

	public int getActorId() {
		return actorId;
	}

	public void setActorId(int actorId) {
		this.actorId = actorId;
	}		
}
