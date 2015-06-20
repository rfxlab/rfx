package rfx.core.util;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class CommonUtil {

	static final String REDIS_CONNECTION_POOL_CONFIG_FILE = "redis-connection-pool-configs.json";
	static final String REDIS_CONFIG_FILE = "redis-configs.json";
	static final String SQL_DB_CONFIG_FILE = "database-configs.json";	
	static final String KAFKA_PRODUCER_CONFIG_FILE = "kafka-producer-configs.json";
	
	static String baseConfig = "configs/";
	public static void setBaseConfig(String baseConfig) {
		CommonUtil.baseConfig = baseConfig;
	}
	
	public static String getRedisPoolConnectionConfigFile(){
		return StringUtil.toString(baseConfig,REDIS_CONNECTION_POOL_CONFIG_FILE);
	}
	
	public static String getRedisConfigFile(){
		return StringUtil.toString(baseConfig,REDIS_CONFIG_FILE);
	}
	
	public static String getSqlDbConfigFile(){
		return StringUtil.toString(baseConfig,SQL_DB_CONFIG_FILE);
	}
	
	public static String getKafkaProducerConfigFile(){
		return StringUtil.toString(baseConfig,KAFKA_PRODUCER_CONFIG_FILE);
	}
	

	public static class COLOR_CODE {
		public static final String ANSI_RESET = "\u001B[0m";
		public static final String ANSI_BLACK = "\u001B[30m";
		public static final String ANSI_RED = "\u001B[31m";
		public static final String ANSI_GREEN = "\u001B[32m";
		public static final String ANSI_YELLOW = "\u001B[33m";
		public static final String ANSI_BLUE = "\u001B[34m";
		public static final String ANSI_PURPLE = "\u001B[35m";
		public static final String ANSI_CYAN = "\u001B[36m";
		public static final String ANSI_WHITE = "\u001B[37m";
	}
	
	public static final Charset CHARSET_UTF8 = Charset.forName(StringPool.UTF_8);

	public static List<String> CRAWLED_PREFIX_URL = new ArrayList<String>() {	
		{
			add("http://vnexpress.net/");
			add("http://thethao.vnexpress.net/");
		}
	};
}
