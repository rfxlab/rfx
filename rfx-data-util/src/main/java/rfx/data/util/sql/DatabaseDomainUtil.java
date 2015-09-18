package rfx.data.util.sql;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class DatabaseDomainUtil {
	static final ApplicationContext APPLICATION_CONTEXT = new ClassPathXmlApplicationContext("database-domain.xml");
	
	public static ApplicationContext getContext(){		
		return APPLICATION_CONTEXT;
	}
}
