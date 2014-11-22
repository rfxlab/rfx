package rfx.core.stream.exception;

public class InvalidSetTaskDef extends Exception {

	private static final long serialVersionUID = 566098014011863043L;


	public InvalidSetTaskDef(String msg) {
		super(msg);		
	}	
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return String.valueOf(this.getMessage());
	}


}
