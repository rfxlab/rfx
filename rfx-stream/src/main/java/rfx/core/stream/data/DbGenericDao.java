package rfx.core.stream.data;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.sql.DataSource;

import rfx.core.stream.configs.SqlDbConfigs;
import rfx.core.util.LogUtil;



public abstract class DbGenericDao {
	
	protected static ExecutorService dbExecutor = Executors.newSingleThreadExecutor();
	
	protected final static String ERROR_CONNECTION_BUSY = "ORA-00257";
	protected final static String ERROR_CONNECTION_TIME_OUT = "timed out";	
	protected volatile int timeToSleep = 0;//for ORA-00257

	private static DataSource oracleDataSource;
	private static SqlDbConfigs sqlDbConfigs;
	
	public static synchronized SqlDbConfigs getSqlDbConfigs() {
		if(sqlDbConfigs == null){
			sqlDbConfigs = SqlDbConfigs.load("dbOracleLogReportConfigs");
		}
		return sqlDbConfigs;
	}
	
	public static synchronized void resetOracleDataSource() {	
		oracleDataSource = null;		
		oracleDataSource = getSqlDbConfigs().getDataSource();
	}
	
	public static synchronized DataSource getOracleDataSource() {
		if(oracleDataSource == null){
			oracleDataSource = getSqlDbConfigs().getDataSource();
		}
		return oracleDataSource;
	}
	public static void dbExceptionHandler(Throwable exception){
		String msg = exception.getMessage();		
		if(msg.contains(ERROR_CONNECTION_TIME_OUT)){
			resetOracleDataSource();//force reset datasource
		} else if(msg.contains(ERROR_CONNECTION_BUSY)){
			//timeToSleep = 4000;
		}		
		LogUtil.i("DbLogDataManager", msg + " " + exception.toString(), true);
		exception.printStackTrace();	
		if(msg.contains("ORA-06550")){
			return;
		}
	}
	
}
