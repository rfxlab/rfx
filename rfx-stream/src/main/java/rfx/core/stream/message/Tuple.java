package rfx.core.stream.message;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import rfx.core.util.StringUtil;

import com.google.gson.Gson;

/**
 * @author trieu
 * 
 * the object for sending data messages in topology
 *
 */
public class Tuple implements Serializable {
	
	private static final long serialVersionUID = 8916317204032928800L;
	private Map<String, Object> dataMap;
	private Fields fields;
	private String str;

	public Tuple() {
		dataMap = new HashMap<>();
	}	
	
	public Tuple(int size) {
		dataMap = new HashMap<>(size);
	}	
	
	public Tuple(Fields fields){
		dataMap = new HashMap<>(fields.size());
		this.fields = fields;		
	}
	
	public Tuple(Fields fields,Values values){
		dataMap = new HashMap<>(fields.size());
		this.fields = fields;		
		setValues(values);
	}
	
	public void setValues(Values values){
		int size = this.fields.size();
		if(size == values.size()){
			for (int i = 0; i < size; i++) {
				dataMap.put(this.fields.get(i), values.get(i));
			}
		} else {
			throw new IllegalArgumentException("fields.size is not equals values.size");
		}
	}
	
	public void setValues(Map<String, Object> dataMap){
		int size = this.fields.size();
		if(size == dataMap.size()){
			Set<String> keys = dataMap.keySet();
			for (String key : keys) {
				this.dataMap.put(key, dataMap.get(key));
			}			
		} else {
			throw new IllegalArgumentException("fields.size is not equals values.size");
		}
	}


	public void setFieldValue(String field, String value) {
		dataMap.put(field, value);
	}
	
	public void setFieldValue(String field, Object value) {
		dataMap.put(field, value);
	}

	public Map<String, Object> getDataMap() {
		return dataMap;
	}

	public Object getValueByField(String field) {
		return dataMap.get(field);
	}

	
	public String getStringByField(String field){
		return String.valueOf(getValueByField(field));
	}
	
	public String getStringByField(String field, String defaultVal){
		return StringUtil.safeString(getValueByField(field), defaultVal);
	}
	
	public int getIntegerByField(String field){
		return StringUtil.safeParseInt(getValueByField(field));
	}
	
	public int getIntegerByField(String field, int defaultVal){
		return StringUtil.safeParseInt(getValueByField(field), defaultVal);
	}
	
	public long getLongByField(String field){
		return  StringUtil.safeParseLong(getValueByField(field));
	}
	
	public long getLongByField(String field, long defaultVal){
		return StringUtil.safeParseLong(getValueByField(field), defaultVal);
	}
	
	public void clear(){
		dataMap.clear();
	}
	
	@Override
	public String toString() {
		if(str == null){
			str = new Gson().toJson(dataMap);
		}
		return str;
	}
}