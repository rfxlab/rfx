package rfx.data.util.sql;

import java.util.HashMap;
import java.util.Map;

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
		try {
			return Integer.parseInt(get(field)+"");
		} catch (Exception e) {	}
		return 0;
	}
	
	public long getLong(String field){
		try {
			return Long.parseLong(get(field)+"");
		} catch (Exception e) {}
		return 0;
	}
	
	public double getDouble(String field){
		try {
			return Double.parseDouble(get(field)+"");
		} catch (Exception e) {}
		return 0;
	}
	
	
	public String toJson() {	
		return new Gson().toJson(this.map);
	}
}
