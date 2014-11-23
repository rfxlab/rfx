package system.starter;

import java.io.IOException;

import rfx.core.job.ScheduledJobNode;

public class ScheduledJobNodeStarter {
	public static void main(String[] args) throws IOException {	
		int port = 11999;
		String host = "localhost";
				
		try {
			String name = host + "_" + port;
			new ScheduledJobNode(name).start(host, port);					
			
		} catch (Exception e) {			
			e.printStackTrace();
			System.exit(1);
		}
	}
}
