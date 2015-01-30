package rfx.core.ua;

import rfx.core.stream.util.ua.Client;
import rfx.core.stream.util.ua.Parser;

public class TestUserAgentParser {

	public static void main(String[] args) {
		Parser parser = Parser.load();
		String agentString = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/40.0.2214.91 Safari/537.36";
		Client client = parser.parse(agentString);
		System.out.println(client.device.deviceType());
		System.out.println(client.device.family);
		
		String mobileAgent = "Mozilla/5.0 (iPhone; CPU iPhone OS 6_0 like Mac OS X) AppleWebKit/536.26 (KHTML, like Gecko) Version/6.0 Mobile/10A5376e Safari/8536.25";
		Client mclient = parser.parse(mobileAgent);
		System.out.println(mclient.device.deviceType());
		System.out.println(mclient.device.family);
				
		
		String tabletAgent = "Mozilla/5.0 (iPad; CPU OS 6_0 like Mac OS X) AppleWebKit/536.26 (KHTML, like Gecko) Version/6.0 Mobile/10A5376e Safari/8536.25";
		Client tbclient = parser.parse(tabletAgent);
		System.out.println(tbclient.device.deviceType());
		System.out.println(tbclient.device.family);
		System.out.println(tbclient.os.family);		
		
	}
}
