package rfx.core.ua;

import rfx.core.util.useragent.Client;
import rfx.core.util.useragent.Parser;

public class TestUserAgentParser {

	public static void main(String[] args) {
		Parser parser = Parser.load();
		String agentString = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/40.0.2214.91 Safari/537.36";
		Client client = parser.parse(agentString);
		System.out.println(client.device.deviceType());
		System.out.println(client.device.family);
		System.out.println("-------------------------------------");
		
		String mobileAgent = "Mozilla/5.0 (iPhone; CPU iPhone OS 6_0 like Mac OS X) AppleWebKit/536.26 (KHTML, like Gecko) Version/6.0 Mobile/10A5376e Safari/8536.25";
		Client mclient = parser.parse(mobileAgent);
		System.out.println(mclient.device.deviceType());
		System.out.println(mclient.device.family);
		System.out.println("-------------------------------------");
		
		String tabletAgent = "Mozilla/5.0 (iPad; CPU OS 6_0 like Mac OS X) AppleWebKit/536.26 (KHTML, like Gecko) Version/6.0 Mobile/10A5376e Safari/8536.25";
		Client tbclient = parser.parse(tabletAgent);
		System.out.println(tbclient.device.deviceType());
		System.out.println(tbclient.device.family);
		System.out.println(tbclient.os.family);		
		System.out.println("-------------------------------------");
		
		String sonySmartTvAgent = "Opera/9.80 (Linux armv7l; Opera TV Store/5602; Model/Sony-KDL-42W804A SonyCEBrowser/1.0 (KDL-42W804A; CTV2013/PKG4.540GAA; VNM)) Presto/2.12.362 Version/12.11";
		Client tvclient1 = parser.parse(sonySmartTvAgent);		
		System.out.println(tvclient1.device.deviceType());
		System.out.println(tvclient1.device.family.split(" ")[0]);
		System.out.println(tvclient1.os.family);	
		System.out.println("-------------------------------------");
		
		String samsungSmartTvAgent = "Mozilla/5.0 (SMART-TV; Linux; Tizen 2.3) AppleWebkit/538.1 (KHTML, like Gecko) SamsungBrowser/1.0 Safari/538.1";
		Client tvclient2 = parser.parse(samsungSmartTvAgent);		
		System.out.println(tvclient2.device.deviceType());
		System.out.println(tvclient2.device.family.split(" ")[0]);
		System.out.println(tvclient2.os.family);	
		System.out.println("-------------------------------------");
	}
}
