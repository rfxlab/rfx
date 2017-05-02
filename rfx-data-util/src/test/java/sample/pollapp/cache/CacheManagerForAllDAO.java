package sample.pollapp.cache;

import java.util.Arrays;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;

import rfx.data.util.cache.CacheManager;

/**
 * How to use Spring AOP
 * http://www.journaldev.com/2583/spring-aop-example-tutorial-aspect-advice-pointcut-joinpoint-annotations-xml-configuration
 * 
 * @author Trieu.nguyen
 *
 */
@Aspect
public class CacheManagerForAllDAO extends CacheManager {

	final static String daoClasspath = "sample.pollapp.business.dao";
	final static String daoClassNameImpleSuffix = "Impl";
	final static String withinClasspath = "within(" + daoClasspath + ".*)";

	public CacheManagerForAllDAO() {
		System.out.println("---CacheManagerForAllDAO---");
		init();
	}

	@Around(withinClasspath)
	public Object process(ProceedingJoinPoint pJoinPoint) {
		try {
			return super.processMethod(pJoinPoint);
		} catch (Throwable e) {
			e.printStackTrace();
		}
		return null;
	}

	@Before(withinClasspath)
	public void logStringArguments(JoinPoint joinPoint) {
//		System.out.println("Before call method= " + joinPoint.toString());
//		System.out.println("Agruments Passed=" + Arrays.toString(joinPoint.getArgs()));
	}

	public static void init() {
		try {
			init(daoClasspath, daoClassNameImpleSuffix);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
