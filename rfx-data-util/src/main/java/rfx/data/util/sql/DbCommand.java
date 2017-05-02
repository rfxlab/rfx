package rfx.data.util.sql;

import org.springframework.jdbc.core.JdbcTemplate;



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
			System.err.println(e);
		}
		return rs;
	}

	//define the logic at implementer
	protected abstract T build();
}
