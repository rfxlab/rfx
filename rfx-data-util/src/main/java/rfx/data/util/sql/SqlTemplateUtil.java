package rfx.data.util.sql;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.jdbc.core.simple.SimpleJdbcCall;



public class SqlTemplateUtil {
	static String baseFolderPath = "";
	public static final String SQL_STRING_TEMPLATE_FILE = "configs/sql-string-template.txt";
	
	static Map<String, String> mapSqlTpl = new HashMap<>();
	
	static {
		try {
			String str = readFileAsString(SQL_STRING_TEMPLATE_FILE);
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
	
	public static String getRuntimeFolderPath()  {
		if(baseFolderPath.isEmpty()){
			try {
				File dir1 = new File(".");
				baseFolderPath = dir1.getCanonicalPath();
			} catch (IOException e) {			
				e.printStackTrace();
			}
		}		
		return baseFolderPath;
	}
	
	public static String readFileAsString(String uri) throws java.io.IOException {			
		StringBuffer fileData = new StringBuffer(1000);
		String fullpath = getRuntimeFolderPath() + uri.replace("/", File.separator);
		if(!uri.startsWith("/")){
			fullpath = getRuntimeFolderPath() + File.separator + uri.replace("/", File.separator);
		}
		
		//System.out.println(fullpath);
		BufferedReader reader = new BufferedReader(new FileReader(fullpath));
		char[] buf = new char[2048];
		int numRead = 0;
		while ((numRead = reader.read(buf)) != -1) {
			fileData.append(buf, 0, numRead);
		}
		reader.close();
		return fileData.toString();
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
