package rfx.core.model;


public class CallbackResult<T> {
	T result;

	public CallbackResult(T result) {
		super();
		this.result = result;
	}

	public CallbackResult() {
		super();
	}

	public T getResult() {
		return result;
	}

	public void setResult(T result) {
		this.result = result;
	}
	
	@Override
	public String toString() {		
		return String.valueOf(result);
	}
}
