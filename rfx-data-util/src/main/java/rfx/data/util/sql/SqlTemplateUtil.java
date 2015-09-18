package rfx.data.util.sql;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.jdbc.core.simple.SimpleJdbcCall;

import rfx.core.util.FileUtils;



public class SqlTemplateUtil {
	public static final String SQL_STRING_TEMPLATE_FILE = "configs/sql-string-template.txt";
	
	static Map<String, String> mapSqlTpl = new HashMap<>();
	
	static {
		try {
			String str = FileUtils.readFileAsString(SQL_STRING_TEMPLATE_FILE);
			String[] sqlStrTokens = str.split(";");
			for (String sqlStrToken : sqlStrTokens) {
				String[] toks = sqlStrToken.split("=>");
				if(toks.length == 2){
					String sqlKey =  toks[0].trim();
					String sqlVal =  toks[1].trim().replace("\t", " ").replace("\n", " ");
					//System.out.println(sqlKey);System.out.println(sqlVal);
					mapSqlTpl.put(sqlKey, sqlVal);
				}				
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static String getSql(String sqlKey){
		String sql = mapSqlTpl.get(sqlKey);
		if(sql == null){
			throw new IllegalArgumentException("Not found value for "+sqlKey + " in file "+SQL_STRING_TEMPLATE_FILE);
		}
		return sql;
	}
	
	public static SimpleJdbcCall getProcedureJdbcCall(DataSource ds, String spKey){
		return new SimpleJdbcCall(ds).withProcedureName(getSql(spKey));
	}
}
