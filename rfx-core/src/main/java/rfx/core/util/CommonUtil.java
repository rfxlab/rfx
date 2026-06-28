package rfx.core.util;

import java.nio.charset.Charset;


/**
 * Utility class for common configuration and console color codes.
 * Supports loading configuration paths dynamically from System Environment Variables.
 */
public class CommonUtil {

    // Define standard environment variable keys for external configuration

    private static final String ENV_BASE_DIR = "APP_BASE_DIR";
    private static final String ENV_BASE_CONFIG = "APP_BASE_CONFIG_DIR";
    private static final String ENV_REDIS_POOL_FILE = "APP_REDIS_POOL_FILE";
    private static final String ENV_REDIS_FILE = "APP_REDIS_FILE";
    private static final String ENV_DATABASE_FILE = "APP_DB_FILE";
    private static final String ENV_KAFKA_FILE = "APP_KAFKA_FILE";

    // Original hardcoded values retained as safe defaults
    private static final String DEFAULT_REDIS_CONNECTION_POOL_CONFIG_FILE = "redis-connection-pool-configs.json";
    private static final String DEFAULT_REDIS_CONFIG_FILE = "redis-configs.json";
    private static final String DEFAULT_DATABASE_CONFIG_FILE = "database-configs.json"; 
    private static final String DEFAULT_KAFKA_PRODUCER_CONFIG_FILE = "kafka-producer-configs.json";
    private static final String DEFAULT_BASE_CONFIG = "configs/";
    private static final String DEFAULT_BASE_DIR = ".";

    /**
     * Base config directory. 
     * It checks the system environment first. If not found, it uses the default "configs/".
     */
    static String baseConfig = getEnvOrDefault(ENV_BASE_CONFIG, DEFAULT_BASE_CONFIG);

    /**
     * Base directory for the application. 
     * It checks the system environment first. If not found, it uses the default "." (current directory).
     */
    static String baseDir = getEnvOrDefault(ENV_BASE_DIR, DEFAULT_BASE_DIR);

    /**
     * Helper method to fetch environment variables with a fallback default.
     * * @param envKey The environment variable key to look up.
     * @param defaultValue The fallback value if the env variable is missing or empty.
     * @return The resolved configuration value.
     */
    private static String getEnvOrDefault(String envKey, String defaultValue) {
        String envValue = System.getenv(envKey);
        // Return the environment variable if it exists and isn't just whitespace, otherwise use default
        return (envValue != null && !envValue.trim().isEmpty()) ? envValue : defaultValue;
    }

    /**
     * Allows manual override of the base configuration directory at runtime.
     * * @param baseConfig The new base directory path.
     */
    public static void setBaseConfig(String baseConfig) {
        CommonUtil.baseConfig = baseConfig;
    }

    /**
     * Retrieves the current base configuration directory.
     * @return The base configuration directory path.
     */
    public static String getBaseConfig() {
        return CommonUtil.baseConfig;
    }

    /**
     * Allows manual override of the base directory at runtime.
     * @param baseDir The new base directory path.
     */
    public static void setBaseDir(String baseDir) {
        CommonUtil.baseDir = baseDir;
    }
    
    /**
     * Retrieves the current base directory.
     * @return The base directory path.
     */
    public static String getBaseDir() {
        return CommonUtil.baseDir;
    }

    /**
     * Retrieves the Redis Pool Connection config file path.
     */
    public static String getRedisPoolConnectionConfigFile(){
        String fileName = getEnvOrDefault(ENV_REDIS_POOL_FILE, DEFAULT_REDIS_CONNECTION_POOL_CONFIG_FILE);
        return StringUtil.toString(baseConfig, fileName);
    }
    
    /**
     * Retrieves the Redis config file path.
     */
    public static String getRedisConfigFile(){
        String fileName = getEnvOrDefault(ENV_REDIS_FILE, DEFAULT_REDIS_CONFIG_FILE);
        return StringUtil.toString(baseConfig, fileName);
    }
    
    /**
     * Retrieves the Database config file path.
     */
    public static String getDatabaseConfigFile(){
        String fileName = getEnvOrDefault(ENV_DATABASE_FILE, DEFAULT_DATABASE_CONFIG_FILE);
        return StringUtil.toString(baseConfig, fileName);
    }
    
    /**
     * Retrieves the Kafka Producer config file path.
     */
    public static String getKafkaProducerConfigFile(){
        String fileName = getEnvOrDefault(ENV_KAFKA_FILE, DEFAULT_KAFKA_PRODUCER_CONFIG_FILE);
        return StringUtil.toString(baseConfig, fileName);
    }
    
    /**
     * ANSI color codes for formatting console output text.
     */
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
    
    // Standard UTF-8 Charset constant for the application
    public static final Charset CHARSET_UTF8 = Charset.forName(StringPool.UTF_8);

}