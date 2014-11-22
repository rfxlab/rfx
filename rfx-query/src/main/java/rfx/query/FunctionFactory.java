package rfx.query;

import java.io.Serializable;
import java.util.function.Function;

public interface FunctionFactory extends Serializable{
	public Function<ActorData, String> build();
}