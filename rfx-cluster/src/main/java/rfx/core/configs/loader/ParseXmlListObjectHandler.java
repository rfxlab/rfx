package rfx.core.configs.loader;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

public abstract class ParseXmlListObjectHandler implements ParseConfigHandler {
	
	protected Configurable configurableObj;
	protected Element xmlNode;
	protected Document doc;
	

	public ParseConfigHandler setFieldMetadata(Configurable configurableObj,Document doc, Element xmlNode) {		
		this.configurableObj = configurableObj;
		this.doc = doc;
		this.xmlNode = xmlNode;
		return this;
	}
}
