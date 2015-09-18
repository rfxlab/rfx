package rfx.data.util.sql;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.regex.Pattern;



/**
 * the helper class for building java.sql.CallableStatement
 * 
 * @author trieu
 *
 */
public class DbSql {

	CallableStatement cs;
	String bindedSql;
	int index = 0;
	
	public DbSql(Connection con, String sql) throws SQLException {
		super();
		this.cs = con.prepareCall(sql);
		this.bindedSql = sql;		
	}
	
	public DbSql setString(String x) throws SQLException{
		index++;
		cs.setString(index, x);		
		this.bindedSql = bindedSql.replaceFirst(Pattern.quote("?"), "\""+x+"\"");
		return this;
	}
	
	public DbSql setInt(int x) throws SQLException{
		index++;
		cs.setInt(index, x);		
		this.bindedSql = bindedSql.replaceFirst(Pattern.quote("?"), x+"");
		return this;
	}
	
	public DbSql setLong(long x) throws SQLException{
		index++;
		cs.setLong(index, x);		
		this.bindedSql = bindedSql.replaceFirst(Pattern.quote("?"), x+"");
		return this;
	}
	
	public DbSql setDouble(double x) throws SQLException{
		index++;
		cs.setDouble(index, x);		
		this.bindedSql = bindedSql.replaceFirst(Pattern.quote("?"), x+"");
		return this;
	}
	
	public int getParamsCount() {
		return index;
	}
	
	public int executeUpdate() throws SQLException{		
		return this.cs.executeUpdate();
	}
	
	public String getBindedSql() {
		return bindedSql;
	}
	
	public void close(){
		try {
			this.cs.close();
		} catch (Exception e) {}
	}
}
