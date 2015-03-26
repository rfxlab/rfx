package system.starter;

import java.io.IOException;

import rfx.core.job.ScheduledJobNode;
import rfx.core.util.StringUtil;

public class ScheduledJobNodeStarter {
	public static void main(String[] args) throws IOException {
		try {
			String host = "localhost";		
			int port = 11999;
			if(args.length == 2){
				host = args[0];
				port = StringUtil.safeParseInt(args[1]);
			}
			String name = host + "_" + port;
			new ScheduledJobNode(name).start(host, port);
		} catch (Exception e) {			
			e.printStackTrace();
			System.exit(1);
		}
	}
}
