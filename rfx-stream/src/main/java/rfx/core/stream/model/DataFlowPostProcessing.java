package rfx.core.stream.model;

import rfx.core.model.Callback;
import rfx.core.model.CallbackResult;

public abstract class DataFlowPostProcessing implements Callback<String>{
	public abstract CallbackResult<String> call();
}
