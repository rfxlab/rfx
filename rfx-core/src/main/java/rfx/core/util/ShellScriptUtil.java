package rfx.core.util;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class ShellScriptUtil {

	public static String execPHP(String scriptName, String param) {
		StringBuilder output = new StringBuilder();
		Process p = null;
		try {						
			p = Runtime.getRuntime().exec("php " + scriptName + " " + param);
			BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String line;
			while ((line = input.readLine()) != null) {
				output.append(line);
			}
			input.close();			
		} catch (Exception err) {
			err.printStackTrace();
		} finally {
			if(p != null){
				p.destroy();
			}
		}
		return output.toString();
	}
	
	public static void main(String[] args) throws Exception {
		while(true){
			String script = "/opt/test.php";
			String out = ShellScriptUtil.execPHP(script, " --cid 123 --date 2013-03-15 --type total");
			System.out.println(out);
			Thread.sleep(2000);
		}
	}
}
