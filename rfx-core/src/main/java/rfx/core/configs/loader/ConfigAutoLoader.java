package rfx.core.configs.loader;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Properties;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import rfx.core.annotation.AutoInjectedConfig;
import rfx.core.util.CommonUtil;
import rfx.core.util.LogUtil;


public class ConfigAutoLoader {
	
	public static void processListConfigClass(String classpath, Class<?> clazz,	Document doc, Element rootNode) throws Exception {
		Field[] fields = clazz.getDeclaredFields();
		Elements nodes = rootNode.select(" > *");
		boolean assignedInstance = false;
		for (Element node : nodes) {
			//System.out.println(node.nodeName());
			Configurable configurableObj = (Configurable) clazz.newInstance();				
			ParseXmlListObjectHandler fieldHandler = (ParseXmlListObjectHandler) configurableObj.getParseConfigHandler();	
			fieldHandler.setFieldMetadata(configurableObj, doc, node).injectFieldValue();	
			for (Field field : fields) {
				field.setAccessible(true);
				if ( !assignedInstance && Modifier.isStatic(field.getModifiers()) ) {				
					AutoInjectedConfig autoInjectedConfig = field.getAnnotation(AutoInjectedConfig.class); 
					if(autoInjectedConfig != null){
						boolean injectable = classpath.equals(field.getType().getName()) && autoInjectedConfig.injectable();								
						if(injectable){					
							field.set(null, configurableObj);
							assignedInstance = true;
						}				
					}				
				} else {
									
				}
			}
		}	
	}
	
	public static void processObjectConfigClass(String classpath, Class<?> clazz, Configurable configurableObj, 
			Document doc, Element rootNode, ParseXmlObjectHandler fieldHandler) throws Exception {
		Field[] fields = clazz.getDeclaredFields();
		for (Field field : fields) {
			field.setAccessible(true);
			if (!Modifier.isStatic(field.getModifiers())) {				
				fieldHandler.setFieldMetadata(configurableObj, field, doc, rootNode).injectFieldValue();				
			} else {				
				AutoInjectedConfig autoInjectedConfig = field.getAnnotation(AutoInjectedConfig.class); 
				if(autoInjectedConfig != null){
					boolean injectable = classpath.equals(field.getType().getName()) && autoInjectedConfig.injectable();								
					if(injectable){					
						field.set(null, configurableObj);
					}				
				}
			}
		}
	}

	public synchronized final static void load(String filepath) {
		try {
			File input = new File(filepath);
			Document doc = Jsoup.parse(input, "UTF-8");
			Elements elements = doc.getElementsByAttribute("classpath");
			if (elements.size() > 0) {
				Element rootNode = elements.first();				
				String classpath = rootNode.attr("classpath");
				String type = rootNode.attr("type");
				Class<?> clazz = Class.forName(classpath);
								
				System.out.println("--load configs for classpath: "+ classpath);
								
				if("list".equals(type)){
					processListConfigClass(classpath, clazz, doc, rootNode);
				} else {
					Configurable configurableObj = (Configurable) clazz.newInstance();				
					ParseXmlObjectHandler fieldHandler = (ParseXmlObjectHandler) configurableObj.getParseConfigHandler();	
					processObjectConfigClass(classpath, clazz, configurableObj, doc, rootNode, fieldHandler);
				}
								
			}
		} catch (Exception ex) {
			// TODO Auto-generated catch block
			ex.printStackTrace();
			LogUtil.error(ex);
		}
	}
	
	static boolean loadAll = false;
	
	public synchronized final static void loadAll(){
		if(loadAll){
			return;
		}
		String configBasePath = "configs/";		
	    try {
	        //load a properties file from class path, inside static method
	    	Properties prop = new Properties(); 
	    	File commonPropFile = new File("common.properties");	    	
	    	if(commonPropFile.isFile()){
	    		prop.load(new FileInputStream(commonPropFile));
	    		configBasePath = prop.getProperty("config");
	    	}
	    } catch (IOException ex) {
	        ex.printStackTrace();
	    }		
	    CommonUtil.setBaseConfig(configBasePath);
	    String coreXmlConfigPath = configBasePath+"rfx-core/";
	    File dir = new File(coreXmlConfigPath);
	    if( ! dir.isDirectory() ){
	    	throw new IllegalArgumentException("Not valid path "+ coreXmlConfigPath);
	    }
		
	    load( coreXmlConfigPath + "redis-pool-configs.xml" );
	    load( coreXmlConfigPath + "cluster-info-configs.xml" );
	    load( configBasePath + "worker-configs.xml" );
	    
		loadAll = true;
	}
}
