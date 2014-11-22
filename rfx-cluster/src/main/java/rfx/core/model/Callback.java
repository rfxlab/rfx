package rfx.core.model;

public interface Callback<T> {
	public CallbackResult<T> call();
}
