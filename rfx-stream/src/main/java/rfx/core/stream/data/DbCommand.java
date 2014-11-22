package rfx.core.stream.data;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import rfx.core.util.LogUtil;

public abstract class DbCommand<T>  {
	
	protected DataSource dataSource;
	protected CallableStatement cs = null;
	protected Connection con = null;
	
	public DbCommand(DataSource dataSource) {
		super();
		if (dataSource == null) {
			throw new IllegalArgumentException("dataSource is NULL!");
		}
		this.dataSource = dataSource;
		try {
			con = dataSource.getConnection();
		} catch (SQLException e) {
			LogUtil.error(e);
		}
	}
	
	public T execute() {		
		T rs = null;		
		try {			
			if (con != null) {
				rs = build();
			}
		} catch (Exception e) {
			e.printStackTrace();
			LogUtil.error(e);
		} finally {
			if (cs != null) {
				try {
					cs.close();
				} catch (SQLException e1) {
				}
			}
			if (con != null) {
				try {
					con.close();
				} catch (SQLException e1) {
				}
			}
		}
		return rs;
	}

	//define the logic at implementer
	protected abstract T build() throws SQLException;
}
