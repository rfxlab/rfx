package rfx.core.configs.loader;

import java.lang.reflect.Field;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

public abstract class ParseXmlObjectHandler implements ParseConfigHandler {
	protected Configurable configurableObj;
	protected Element xmlNode;
	protected Document doc;
	protected Field field;

	public ParseConfigHandler setFieldMetadata(Configurable configurableObj,Field field,Document doc, Element xmlNode) {		
		this.configurableObj = configurableObj;
		this.field = field;
		this.doc = doc;
		this.xmlNode = xmlNode;
		return this;
	}
}
