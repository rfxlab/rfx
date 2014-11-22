package rfx.core.stream.node.handler;

import java.util.HashMap;
import java.util.Map;

import rfx.core.model.HttpServerRequestCallback;

/**
 * the HTTP Request Handler Mapper 
 * 
 * <br>
 * @author trieu
 *
 */
public abstract class HttpHandlerMapper {
	static Map<String, HttpServerRequestCallback> callbackHandlers = new HashMap<>();
	static Map<Class<?>, HttpHandlerMapper> mappers = new HashMap<>();
	
	static void when(String path, HttpServerRequestCallback callback){
		if(callbackHandlers.containsKey(path)){
			throw new IllegalArgumentException("Duplicated HttpServerRequestCallback for path "+path);
		}
		callbackHandlers.put(path, callback);
	}
	public HttpServerRequestCallback getByPath(String path){	
		System.out.println("getByPath: " + path);
		return callbackHandlers.get(path.toLowerCase());
	}
	
	public static HttpHandlerMapper get(Class<?> clazz) throws InstantiationException, IllegalAccessException{
		HttpHandlerMapper mapper = mappers.get(clazz);
		if(mapper == null){
			mapper = (HttpHandlerMapper) clazz.newInstance();
			mapper.buildCallbackHandlers();
			mappers.put(clazz, mapper);
		}
		return mapper;
	}
	
	protected abstract HttpHandlerMapper buildCallbackHandlers();
	
}
