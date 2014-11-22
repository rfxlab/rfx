package rfx.core.stream.exception;

import rfx.core.model.WorkerInfo;

public class WorkerInfoException  extends Exception {

	private static final long serialVersionUID = 1471885155437432005L;
	WorkerInfo workerInfo;
	
	public WorkerInfoException(String msg ) {
		super(msg);
	}	
	
	public WorkerInfoException(String msg, WorkerInfo workerInfo ) {
		super(msg);	
		this.workerInfo = workerInfo;
	}	
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		if(workerInfo != null){
			return String.valueOf(this.getMessage()+" by "+workerInfo);
		}
		return this.getMessage();
	}

}
