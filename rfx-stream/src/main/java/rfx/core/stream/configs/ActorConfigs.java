package rfx.core.stream.configs;

import java.io.File;

import rfx.core.util.CommonUtil;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class ActorConfigs {

	public static Config getDefault(){
		try {
			return ConfigFactory.parseFile(new File(CommonUtil.ACTOR_SYSTEM_CONFIG_FILE));
		} catch (Exception e) {
			System.err.println("Can NOT load file "+CommonUtil.ACTOR_SYSTEM_CONFIG_FILE);
			System.exit(1);
		}
		return null;
	}
}
