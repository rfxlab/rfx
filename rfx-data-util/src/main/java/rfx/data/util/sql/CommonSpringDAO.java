package rfx.data.util.sql;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.function.Function;

import javax.inject.Inject;
import javax.inject.Named;
import javax.sql.DataSource;

import org.springframework.jdbc.core.JdbcTemplate;

import rfx.data.util.json.JSONArray;
import rfx.data.util.json.JSONException;
import rfx.data.util.json.JSONObject;

/**
 * refer more at http://docs.spring.io/spring/docs/current/spring-framework-reference/html/jdbc.html
 * 
 * @author Trieu.nguyen
 *
 */
public class CommonSpringDAO {

	protected JdbcTemplate jdbcTpl;
	protected DataSource dataSource;
	protected Function<DataSource, Boolean> setDataSourceCallback; 

	@Inject
    @Named("dataSource")
	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
		this.jdbcTpl = new JdbcTemplate(this.dataSource);
		if(setDataSourceCallback != null){
			setDataSourceCallback.apply(dataSource);
		}
	}
	
	public JdbcTemplate getJdbcTemplate() {
		return jdbcTpl;
	}
	
	private static JSONObject toJSONObject(ResultSetMetaData rsmd, int numColumns, ResultSet rs) throws JSONException, SQLException{
		JSONObject obj = new JSONObject();
		for (int i = 1; i < numColumns + 1; i++) {
			String column_name = rsmd.getColumnName(i);

			switch (rsmd.getColumnType(i)) {
			case java.sql.Types.ARRAY:
				obj.put(column_name, rs.getArray(i));
				break;
			case java.sql.Types.BIGINT:
				obj.put(column_name, rs.getInt(i));
				break;
			case java.sql.Types.BOOLEAN:
				obj.put(column_name, rs.getBoolean(i));
				break;
			case java.sql.Types.BLOB:
				obj.put(column_name, rs.getBlob(i));
				break;
			case java.sql.Types.DOUBLE:
				obj.put(column_name, rs.getDouble(i));
				break;
			case java.sql.Types.FLOAT:
				obj.put(column_name, rs.getFloat(i));
				break;
			case java.sql.Types.INTEGER:
				obj.put(column_name, rs.getInt(i));
				break;
			case java.sql.Types.NVARCHAR:
				obj.put(column_name, rs.getNString(i));
				break;
			case java.sql.Types.VARCHAR:
				obj.put(column_name, rs.getString(i));
				break;
			case java.sql.Types.TINYINT:
				obj.put(column_name, rs.getInt(i));
				break;
			case java.sql.Types.SMALLINT:
				obj.put(column_name, rs.getInt(i));
				break;
			case java.sql.Types.DATE:
				obj.put(column_name, rs.getDate(i));
				break;
			case java.sql.Types.TIMESTAMP:
				obj.put(column_name, rs.getTimestamp(i));
				break;
			default:
				obj.put(column_name, rs.getObject(i));
				break;
			}
		}
		return obj;
	}
	
	public static JSONObject convertResultSetToJSONObject(ResultSet rs) {
		try {
			ResultSetMetaData rsmd = rs.getMetaData();
			while (rs.next()) {
				int numColumns = rsmd.getColumnCount();				
				return toJSONObject(rsmd, numColumns, rs);
			}
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return new JSONObject();
	}

	public static JSONArray convertResultSetToJSONArray(ResultSet rs) {
		JSONArray jsonArray = new JSONArray();
		try {
			ResultSetMetaData rsmd = rs.getMetaData();
			while (rs.next()) {
				int numColumns = rsmd.getColumnCount();				
				jsonArray.put(toJSONObject(rsmd, numColumns, rs));
			}
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return jsonArray;
	}
}
