package server.http.configs;

import java.io.IOException;

import rfx.core.util.FileUtils;

public abstract class HttpLogServerCommon {
	public static final String KAFKA_PRODUCER_CONFIG_FILE = "configs/kafka-producer-handler.json";
	
    public static String getConfigAsText(String name) throws IOException{
    	return FileUtils.readFileAsString(name);
    }
}
