package rfx.data.util.sql;

import java.util.HashMap;
import java.util.Map;

import rfx.core.util.StringUtil;

import com.google.gson.Gson;

/**
 * General object for database access & retrieval
 * 
 * @author Trieu.nguyen
 *
 */
public class DbObject {

	private Map<String, Object> map;
	
	public DbObject() {
		map = new HashMap<String, Object>();
	}
	
	public DbObject(int field) {
		map = new HashMap<String, Object>(field);
	}
	
	public void set(String field, Object val){
		this.map.put(field, val);
	}
	
	public void put(String field, Object val){
		this.map.put(field, val);
	}
	
	public void put(String field, String val){
		this.map.put(field, val);
	}
	
	public void put(String field, int val){
		this.map.put(field, val);
	}
	
	public void put(String field, long val){
		this.map.put(field, val);
	}
	
	public void put(String field, double val){
		this.map.put(field, val);
	}
	
	public Object get(String field){
		return map.get(field);
	}
	
	public String getString(String field){
		return String.valueOf(get(field));
	}
	
	public int getInt(String field){
		return StringUtil.safeParseInt(get(field));
	}
	
	public long getLong(String field){
		return StringUtil.safeParseLong(get(field));
	}
	
	public double getDouble(String field){
		return StringUtil.safeParseDouble(get(field)+"");
	}
	
	
	public String toJson() {	
		return new Gson().toJson(this.map);
	}
}
