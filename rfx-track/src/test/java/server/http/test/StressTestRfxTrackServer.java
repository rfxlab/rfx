package server.http.test;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.databene.contiperf.PerfTest;
import org.databene.contiperf.Required;
import org.databene.contiperf.junit.ContiPerfRule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.base.Stopwatch;

public class StressTestRfxTrackServer {
	
	static AtomicInteger validCount = new AtomicInteger(0);
	static AtomicInteger invalidCount = new AtomicInteger(0);
	static Stopwatch stopwatch = Stopwatch.createUnstarted();

	@Rule
	public ContiPerfRule i = new ContiPerfRule();
		
	@Test
	@PerfTest(invocations = 30000, threads = 300)
	@Required(max = 4000, average = 100)
	public void testPing() throws Exception {
		final String url = "http://log.eclick.vn/ping";
		final String PONG = "PONG";		
		String rs = HttpClientCaller.executeGet(url);
		if (rs.equals(PONG)) {
			int c = validCount.incrementAndGet();
			System.out.println("c = "+c);
		} else {
			//System.out.println(rs);
			invalidCount.incrementAndGet();
		}
	}
	
	
	@PerfTest(invocations = 30000, threads = 300)
	@Required(max = 4000, average = 100)
	public void testTracking() throws Exception {
		final String url = "http://localhost:8080/l?kp=c&url=http://en.wikipedia.org/wiki/Sensor_web";
				
		String rs = HttpClientCaller.executeGet(url);
		if (rs.length()>3) {
			int c = validCount.incrementAndGet();
			//System.out.println("c = "+c);
		} else {
			System.out.println(rs);
			invalidCount.incrementAndGet();
		}
	}
	
	@Before
	public void beginTest(){
		stopwatch.start();
		System.out.println("-------------------------------------");
		System.out.println("valid " + validCount.get());
		System.out.println("invalid " + invalidCount.get());
		System.out.println("-------------------------------------");
	}
	
	@After
	public void finishTest(){
		System.out.println("-------------------------------------");
		System.out.println("valid " + validCount.get());
		System.out.println("invalid " + invalidCount.get());
		System.out.println("-------------------------------------");
		stopwatch.stop();
		System.out.println("finished in milliseconds: "+stopwatch.elapsed(TimeUnit.MILLISECONDS));
	}
	
	//Throughput: 10,186/s . Started at: Jan 24, 2015 2:33:37 PM

}
