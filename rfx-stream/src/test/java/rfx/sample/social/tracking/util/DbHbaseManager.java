package rfx.sample.social.tracking.util;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import rfx.core.stream.configs.SqlDbConfigs;

public class DbHbaseManager {
	private static SqlDbConfigs sqlDbConfigs;

	public DbHbaseManager(SqlDbConfigs sqlDbConfigs) {
		super();
		if (DbHbaseManager.sqlDbConfigs == null) {
			DbHbaseManager.sqlDbConfigs = sqlDbConfigs;
		}
	}

	public static synchronized SqlDbConfigs getSqlDbConfigs() {
		if (sqlDbConfigs == null) {
			sqlDbConfigs = SqlDbConfigs.load("dbHbaseConfigs");
		}
		return sqlDbConfigs;
	}

	public static Connection getDBConnection() {

		Connection dbConnection = null;

		try {

			Class.forName(getSqlDbConfigs().getDbdriverclasspath());

		} catch (ClassNotFoundException e) {
			System.out.println(e.getMessage());
		}

		try {

			dbConnection = DriverManager.getConnection("jdbc:phoenix:"
					+ getSqlDbConfigs().getHost(), getSqlDbConfigs()
					.getUsername(), getSqlDbConfigs().getPassword());
			return dbConnection;

		} catch (SQLException e) {

			System.out.println(e.getMessage());

		}

		return dbConnection;
	}
	
	static void testUpsertRecord() throws SQLException{
		Connection dbConnection = null;
		dbConnection = DbHbaseManager.getDBConnection();		
		String sql = "UPSERT INTO WEB_STAT VALUES('US','Apple.com','iPhone', CURRENT_DATE(), 12, 22, 900)";
		if (dbConnection != null) {
			System.out.println("connect success");
			PreparedStatement preStat = null;
			try {
				dbConnection = DbHbaseManager.getDBConnection();
				preStat = dbConnection.prepareStatement(sql);	
				int c = preStat.executeUpdate();
				System.out.println(c + " record updated!");
				dbConnection.commit();
			} catch (Exception e) {
				System.out.println(e + e.getMessage());
				dbConnection.rollback();
				throw new RuntimeException("hbase query error");
			} finally {
				if (preStat != null) {
					preStat.close();
				}
				if (dbConnection != null) {
					dbConnection.close();
				}
			}
		} else {
			System.out.println("connect fail");
		}
	}
	
	static void testQueryRecord() throws SQLException{
		Connection dbConnection = null;
		dbConnection = DbHbaseManager.getDBConnection();		
		String sql = "SELECT * FROM WEB_STAT WHERE DOMAIN = 'Apple.com' AND FEATURE = 'iPhone'";
		if (dbConnection != null) {
			System.out.println("connect success");
			PreparedStatement preStat = null;
			try {
				dbConnection = DbHbaseManager.getDBConnection();
				preStat = dbConnection.prepareStatement(sql);	
				ResultSet rs = preStat.executeQuery();
				while(rs.next()){
					String host = rs.getString("HOST");
					String feature = rs.getString("FEATURE");
					int v = rs.getInt("ACTIVE_VISITOR");
					Date date = rs.getDate("DATE");
					System.out.println(date + " " + host + " " + feature + " ACTIVE_VISITOR: "+v);	
				}
			} catch (Exception e) {
				System.out.println(e + e.getMessage());				
				throw new RuntimeException("hbase query error");
			} finally {
				if (preStat != null) {
					preStat.close();
				}
				if (dbConnection != null) {
					dbConnection.close();
				}
			}
		} else {
			System.out.println("connect fail");
		}
	}
	
	static void testUpsertRecord2() throws SQLException, ParseException{
		Connection dbConnection = null;
		dbConnection = DbHbaseManager.getDBConnection();		
		String sql = "UPSERT INTO USER_LOG(UUID,URL,DATE,PAGEVIEW_STATS,CLICK_STATS) VALUES(?,?,?,?,?)";
		
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");        
		Date date = new Date(format.parse("2014-10-03").getTime());
		if (dbConnection != null) {
			System.out.println("connect success");
			PreparedStatement preStat = null;
			try {
				dbConnection = DbHbaseManager.getDBConnection();
				preStat = dbConnection.prepareStatement(sql);
				
				preStat.setString(1, "c64dc9bab1431104");
				preStat.setString(2, "http://sohoa.vnexpress.net");
				preStat.setDate(3, date);
				preStat.setLong(4, 156);
				preStat.setLong(5, 10);
				//preStat.setLong(6, 312);
				
				int c = preStat.executeUpdate();
				System.out.println(c + " record updated!");
				dbConnection.commit();
			} catch (Exception e) {
				System.out.println(e + e.getMessage());
				dbConnection.rollback();
				throw new RuntimeException("hbase query error");
			} finally {
				if (preStat != null) {
					preStat.close();
				}
				if (dbConnection != null) {
					dbConnection.close();
				}
			}
		} else {
			System.out.println("connect fail");
		}
	}
	
	static void testQueryRecord2() throws SQLException{
		Connection dbConnection = null;
		dbConnection = DbHbaseManager.getDBConnection();		
		String sql = "SELECT * FROM USER_LOG(FB_STATS BIGINT) ";
		String tab = "   ";
		if (dbConnection != null) {
			System.out.println("connect success");
			PreparedStatement preStat = null;
			try {
				dbConnection = DbHbaseManager.getDBConnection();
				preStat = dbConnection.prepareStatement(sql);	
				ResultSet rs = preStat.executeQuery();
				ResultSetMetaData rsmd = rs.getMetaData();
				int columnCount = rsmd.getColumnCount();

				// The column count starts from 1
				for (int i = 1; i < columnCount + 1; i++ ) {
					String name = rsmd.getColumnName(i);
					System.out.print(name + tab);
				}
				System.out.println();
				while(rs.next()){
					String uuid = rs.getString("UUID");
					String url = rs.getString("URL");
					Date date = rs.getDate("DATE");
					long pageviewStats = rs.getLong("PAGEVIEW_STATS");
					long clickStats = rs.getLong("CLICK_STATS");
					long fbStats = rs.getLong("FB_STATS");
					
					System.out.print(uuid + tab + url + tab + date + tab +pageviewStats + tab+clickStats + tab + fbStats + "\n");	
				}
			} catch (Exception e) {
				System.out.println(e + e.getMessage());				
				throw new RuntimeException("hbase query error");
			} finally {
				if (preStat != null) {
					preStat.close();
				}
				if (dbConnection != null) {
					dbConnection.close();
				}
			}
		} else {
			System.out.println("connect fail");
		}
	}

	public static void main(String[] args) throws Exception {
		testUpsertRecord2();
		testQueryRecord2();
	}
}
