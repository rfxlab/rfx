package rfx.query;

import java.io.Serializable;
import java.util.function.Predicate;

import com.google.gson.Gson;

public class QueryMessage implements Serializable, Comparable<QueryMessage> {
	private static final long serialVersionUID = 755085700797282107L;
	Predicate<?> predicate;
	int id;
	
	public QueryMessage(Predicate<?> predicate, int id) {
		super();
		this.predicate = predicate;
		this.id = id;
	}
	public Predicate<?> getQuery() {
		return predicate;
	}

	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	
	@Override
	public String toString() {
		return new Gson().toJson(this);
	}
	@Override
	public int compareTo(QueryMessage o) {
		if(id == o.getId()){
			return 0;
		} else if(id>o.getId()){
			return 1;
		} else {
			return -1;
		}
	}
}
