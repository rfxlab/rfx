package rfx.data.util.sql;

import org.springframework.jdbc.core.JdbcTemplate;

import rfx.core.util.LogUtil;



public abstract class DbCommand<T>  {
		
	protected JdbcTemplate jdbcTpl;
	
		
	public DbCommand(CommonSpringDAO dbGenericDao) {
		super();
		if (dbGenericDao == null) {
			throw new IllegalArgumentException("dbGenericDao is NULL!");
		}
		jdbcTpl = dbGenericDao.getJdbcTemplate();
	}
	
	public T execute() {		
		T rs = null;		
		try {			
			if (jdbcTpl != null) {
				rs = build();
			} else {
				System.err.println("jdbcTpl is NULL!");
			}
		} catch (Exception e) {
			e.printStackTrace();
			LogUtil.error(e);
		}
		return rs;
	}

	//define the logic at implementer
	protected abstract T build();
}
