package rfx.core.stream.configs;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;

import rfx.core.util.CommonUtil;
import rfx.core.util.FileUtils;
import rfx.core.util.StringPool;
import rfx.core.util.StringUtil;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;


public class SqlDbConfigs implements Serializable {

	private static final long serialVersionUID = 6185084071488833500L;
	
	public static final int MAX_CONNECTIONS = 100;	
	public static final int MIN_CONNECTIONS = 2;
	public static final long MAX_WAIT = 15000;
	
	public final static String MY_SQL = "mysql";
	public final static String SQL_SERVER = "sqlserver";
	public final static String ORACLE = "oracle";
	public final static String ORACLE_TIMESTEN = "timedb";
	
	
	public static class SqlDbConfigsMap {
		private HashMap<String, SqlDbConfigs> map;
		public SqlDbConfigsMap() {}
		public HashMap<String, SqlDbConfigs> getMap() {
			if(map == null){
				map = new HashMap<String, SqlDbConfigs>(0);
			}
			return map;
		}
		public void setMap(HashMap<String, SqlDbConfigs> map) {
			this.map = map;
		}		
	}
	
	String dbId = StringPool.BLANK;
	private String username;
	private String password;
	private String database;
	private String host;
	private int port;
	private String dbdriver;
	private String dbdriverclasspath;
	
	
	static final Map<String,SqlDbConfigs> sqlDbConfigsCache = new HashMap<String,SqlDbConfigs>(6);
	static final Map<String,Driver> driversCache = new HashMap<String, Driver>();
	static String sqlDbConfigsJson = null;
	
	public static SqlDbConfigs load(String dbId){		
		return loadFromFile(CommonUtil.getSqlDbConfigFile(),dbId);
	}
	
	public static SqlDbConfigs loadConfigs(String sqlDbConfigsJson, String dbId){
		String k = dbId;
		if( ! sqlDbConfigsCache.containsKey(k) ){
			SqlDbConfigs configs = null;
			try {						
				SqlDbConfigsMap map =  new Gson().fromJson(sqlDbConfigsJson, SqlDbConfigsMap.class);			
				configs = map.getMap().get(dbId);				
			} catch (Exception e) {
				e.printStackTrace();
			}
			if(configs == null){
				throw new IllegalArgumentException("CAN NOT LOAD SqlDbConfigs from JSON: " + sqlDbConfigsJson);
			}
			configs.setDbId(dbId);
			sqlDbConfigsCache.put(k, configs);
		}		
		return sqlDbConfigsCache.get(k);
	}
	
	public static SqlDbConfigs loadFromFile(String filePath, String dbId){		
		String k = dbId;
		if(sqlDbConfigsJson == null){
			try {
				sqlDbConfigsJson = FileUtils.readFileAsString(filePath);
			} catch (IOException e) {
				throw new IllegalArgumentException("File is not found at " + filePath);
			}
		}
		if( ! sqlDbConfigsCache.containsKey(k) ){
			SqlDbConfigs configs = null;
			try {						
				SqlDbConfigsMap map =  new Gson().fromJson(sqlDbConfigsJson, SqlDbConfigsMap.class);			
				configs = map.getMap().get(dbId);				
			} catch (Exception e) {
				if(e instanceof JsonSyntaxException){
					System.err.println("Wrong JSON syntax in file " + filePath );
				}else {
					e.printStackTrace();	
				}				
			}
			if(configs == null){
				throw new IllegalArgumentException("CAN NOT LOAD SqlDbConfigs dbId " + dbId + " from " + filePath );
			}
			configs.setDbId(dbId);
			sqlDbConfigsCache.put(k, configs);
		}		
		return sqlDbConfigsCache.get(k);
	}
	
	
	public String getConnectionUrl(){
		StringBuilder s = new StringBuilder();		
		if(MY_SQL.equals(this.getDbdriver())){
			s.append("jdbc:").append(MY_SQL).append("://");
			s.append(this.getHost());
			s.append(":").append(getPort()).append("/");
			s.append(this.getDatabase());
			s.append("?autoReconnect=true&useUnicode=true&characterEncoding=UTF-8&noAccessToProcedureBodies=true");			
		} else if(SQL_SERVER.equals(this.getDbdriver())){
			s.append("jdbc:").append(SQL_SERVER).append("://");
			s.append(this.getHost());
			s.append(";databaseName=");
			s.append(this.getDatabase());
		} else if(ORACLE.equals(this.getDbdriver())){
			System.setProperty("java.security.egd", "file:///dev/urandom");
			//"jdbc:oracle:thin:@10.254.53.220:1521:ORADEV1"
			s.append("jdbc:").append(ORACLE).append(":thin:@");
			s.append(this.getHost());
			s.append(":").append(this.getPort()).append(":");
			s.append(this.getDatabase());
		} else if(ORACLE_TIMESTEN.equals(this.getDbdriver())){
			//DriverManager.getConnection("jdbc:timedb://localhost:3601", "db-username", "db-password");
			s.append("jdbc:").append(ORACLE_TIMESTEN).append("://");
			s.append(this.getHost());
			s.append(":").append(this.getPort()).append(":");
			s.append(this.getDatabase());
		}		
		else {
			throw new IllegalArgumentException("Currently, only support JDBC driver for MySQL, MSSQL Server and Oracle!");
		}		
		return s.toString();
	}
	
	public Connection getConnection() throws SQLException{
		Connection dbConnection = null;
		try {			
			Driver driver = driversCache.get(getDbdriverclasspath());
			String connectionUrl = getConnectionUrl();
			if(driver == null){
				driver = (Driver) Class.forName(getDbdriverclasspath()).newInstance();
				driversCache.put(getDbdriverclasspath(), driver);
				DriverManager.registerDriver(driver);
			}			
			dbConnection = DriverManager.getConnection(connectionUrl, getUsername(), getPassword());
		} catch (ClassNotFoundException e) {
			throw new IllegalArgumentException("Missing JDBC driver jar for " + this.getDbdriverclasspath());
		} catch (InstantiationException e) {			
			e.printStackTrace();
		} catch (IllegalAccessException e) {		
			e.printStackTrace();
		} 
		
		return dbConnection;
	}
	
	
	
	public SqlDbConfigs() {
	}
	
	public String getUsername() {
		return username;
	}
	public void setUsername(String username) {
		this.username = username;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	public String getDatabase() {
		return database;
	}
	public void setDatabase(String database) {
		this.database = database;
	}
	public String getHost() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}
	public String getDbdriver() {
		return dbdriver;
	}
	public void setDbdriver(String dbdriver) {
		this.dbdriver = dbdriver;
	}
	public String getDbdriverclasspath() {
		return dbdriverclasspath;
	}
	public void setDbdriverclasspath(String dbdriverclasspath) {
		this.dbdriverclasspath = dbdriverclasspath;
	}	
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}	
	public String getDbId() {
		return dbId;
	}
	public void setDbId(String dbId) {
		this.dbId = dbId;
	}

	public DataSource getDataSource() throws IllegalArgumentException{
		String connectionUrl = getConnectionUrl();
		if(StringUtil.isEmpty(dbdriverclasspath)
				|| getPort() == 0
				|| StringUtil.isEmpty(database)
				|| StringUtil.isEmpty(username) 
				|| StringUtil.isEmpty(password)  
				|| StringUtil.isEmpty(connectionUrl)){
			throw new IllegalArgumentException("Some of sql config is not valid, can not create DataSource for dbId: " + dbId);
		}
		DriverManager.setLoginTimeout(30);
		BasicDataSource dataSource = new BasicDataSource();
		dataSource.setInitialSize(SqlDbConfigs.MIN_CONNECTIONS);
		dataSource.setDriverClassName(getDbdriverclasspath());
		dataSource.setUsername(getUsername());
		dataSource.setPassword(getPassword());
		dataSource.setUrl(connectionUrl);	
		System.out.println(dbdriverclasspath+" connectionUrl:"+connectionUrl);
		
		dataSource.setTestOnBorrow(true);
		dataSource.setTestOnReturn(true);
		dataSource.setTestWhileIdle(true);			
		dataSource.setMaxActive(SqlDbConfigs.MAX_CONNECTIONS);
		dataSource.setMinIdle(SqlDbConfigs.MIN_CONNECTIONS);
		dataSource.setMaxWait(SqlDbConfigs.MAX_WAIT);
		dataSource.setDefaultAutoCommit(true);		
		return dataSource;	
		
	}

	@Override
	public String toString() {
		StringBuilder s = new StringBuilder();
		s.append(" ,Dbdriver: ").append(getDbdriver());
		s.append(" ,Dbdriverclasspath: ").append(getDbdriverclasspath());
		s.append(" ,Host: ").append(getHost());
		s.append(" ,Database: ").append(getDatabase());
		s.append(" ,Username: ").append(getUsername());
		s.append(" ,Password.length: ").append(getPassword().length());
		s.append(" ,ConnectionUrl: ").append(getConnectionUrl());
		return s.toString();
	}
}
