package rfx.core.model;

import java.util.Map;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.VoidHandler;
import org.vertx.java.core.http.HttpServerRequest;

import rfx.core.util.StringPool;

public abstract class HttpServerRequestCallback implements Callback<String> {
	
	static final String FILTER_MESSAGE = "Filtered HTTP Request";
	protected HttpServerRequest request;
	protected boolean isHttpPost = false;

	public CallbackResult<String> handleHttpGetRequest(HttpServerRequest request) {
		this.request = request;
		this.isHttpPost = false;
		return call();
	}
	
	public void handleHttpPostRequest(HttpServerRequest request) {
		this.request = request;
		this.isHttpPost = true;
		call();
	}
	
	protected boolean filterValidRequest(){
		StringBuilder sb = new StringBuilder();
		for (Map.Entry<String, String> header : request.headers().entries()) {
			sb.append(header.getKey()).append(": ")
					.append(header.getValue()).append("\n");
		}
		request.response().putHeader("content-type", "text/plain");
		//System.out.println(sb.toString());
		//TODO
		if("me".equals(request.params().get("filter"))){
			return false;
		}
		return true;
	}
	
	protected String onHttpGetOk() {
		return StringPool.BLANK;
	}
	
	protected String onHttpPostOk(MultiMap formData) {
		return StringPool.BLANK;
	}
	
	protected String onFiltered(){
		return FILTER_MESSAGE;
	}
	
	protected String onError(Throwable e){
		return ExceptionUtils.getStackTrace(e);
	}

	@Override
	public CallbackResult<String> call() {		
		try {
			if(filterValidRequest()){
				if(isHttpPost){
					request.endHandler(new VoidHandler() {
					    public void handle() {
					        // The request has been all ready so now we can look at the form attributes
					    	String rs = onHttpPostOk(request.formAttributes());
					    	if(rs != null){
					    		request.response().end(rs);
					    	} else {
					    		request.response().end(StringPool.BLANK);
					    	}
					    }
					});
					return null;
				} else {
					return new CallbackResult<String>(this.onHttpGetOk());
				}
			} else {
				return new CallbackResult<String>(this.onFiltered());
			}
		} catch (Throwable e) {
			//e.printStackTrace();
			String rs = onError(e);
			if(rs != null){
				System.err.println(rs);
				return new CallbackResult<String>(rs);
			}
			return new CallbackResult<String>(e.getMessage());
		}		
	}
}
