package rfx.data.util.cache;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 
 * 
 * the annotation for method only, get metadata about expire time and keyPrefix (default is methodName) <br>
 * if (expireAfter ==  REQUEST_SESSION) the cachable data of method will be invalidated after finishing response to client 
 *  <br>
 * @author trieu <br>
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface CachableMethod {
	public final static long HTTP_EVENT_PROCESSING_DONE = 0;
	long expireAfter() default HTTP_EVENT_PROCESSING_DONE;
	String keyPrefix() default "";
}