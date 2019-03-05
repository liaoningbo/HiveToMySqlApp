package hualala.com.utils;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import javax.sql.DataSource;
import org.apache.commons.dbcp.BasicDataSourceFactory;

public class DBCPUtil {
	private static DataSource tidbds;
	private static DataSource mysqlds;
    private static DataSource mysqlds_supply;
    private static DataSource mysqlds_db_data;
	private static DataSource mysqlds_db_hedw;

	public static String db_hedw_jdbcUrl = null;
	public static String db_hedw_jdbcUsername = null;
	public static String db_hedw_jdbcPassword = null;

	public static String tidb_jdbcUrl = null;
	public static String tidb_jdbcUsername = null;
	public static String tidb_jdbcPassword = null;

	public static String mysql_jdbcUrl = null;
	public static String mysql_jdbcUsername = null;
	public static String mysql_jdbcPassword = null;

    private static String mysql_jdbcUrl_suppy = null;
	private static String mysql_jdbcUsername_supply = null;
	private static String mysql_jdbcPassword_supply = null;

	private static String mysql_jdbcUrl_db_data = null;
	private static String mysql_jdbcUsername_db_data = null;
	private static String mysql_jdbcPassword_db_data = null;

	private  static  String driverClassName=null;
	static {
		try {
			int runMode = ConfigurationManager.getIntegerProperty(Constants.SPARK_JOB_RUN_MODE);
			InputStream in = null;
			if(runMode == 0) {//本地开发
				in = DBCPUtil.class.getClassLoader().getResourceAsStream("local/db/dbcp.properties");
			} else if(runMode == 1) {//测试环境
				in = DBCPUtil.class.getClassLoader().getResourceAsStream("test/db/dbcp.properties");
			} else if (runMode == 2) {//生产环境
				in = DBCPUtil.class.getClassLoader().getResourceAsStream("production/db/dbcp.properties");
			} else {
				throw new ExceptionInInitializerError("你脑子逛街去了？");
			}
			Properties prop = new Properties();
			prop.load(in);
			driverClassName=prop.getProperty(Constants.JDBC_DRIVER_CLASSNAME);
			tidb_jdbcUrl = prop.getProperty(Constants.TIDB_JDBC_URL);
			tidb_jdbcUsername = prop.getProperty(Constants.TIDB_JDBC_USERNAME);
			tidb_jdbcPassword = prop.getProperty(Constants.TIDB_JDBC_PASSWORD);
			Properties setTidbProp = new Properties();
			setTidbProp.setProperty("driverClassName",driverClassName);
			setTidbProp.setProperty("url",tidb_jdbcUrl);
			setTidbProp.setProperty("username",tidb_jdbcUsername);
			setTidbProp.setProperty("password",tidb_jdbcPassword);
			setTidbProp.setProperty("maxActive","50");
			setTidbProp.setProperty("maxIdle","50");
			setTidbProp.setProperty("minIdle","5");
			setTidbProp.setProperty("maxWait","90000");
			setTidbProp.setProperty("validationQuery","SELECT 1");
			setTidbProp.setProperty("testOnBorrow","true");
			setTidbProp.setProperty("testWhileIdle","true");
			setTidbProp.setProperty("timeBetweenEvictionRunsMillis","3600000");
			setTidbProp.setProperty("minEvictableIdleTimeMillis","18000000");
			tidbds = BasicDataSourceFactory.createDataSource(setTidbProp);
			mysql_jdbcUrl = prop.getProperty(Constants.MYSQL_JDBC_URL);
			mysql_jdbcUsername = prop.getProperty(Constants.MYSQL_JDBC_USERNAME);
			mysql_jdbcPassword = prop.getProperty(Constants.MYSQL_JDBC_PASSWORD);
			Properties setMysqlProp = new Properties();
			setMysqlProp.setProperty("driverClassName",driverClassName);
			setMysqlProp.setProperty("url",mysql_jdbcUrl);
			setMysqlProp.setProperty("username",mysql_jdbcUsername);
			setMysqlProp.setProperty("password",mysql_jdbcPassword);
			setMysqlProp.setProperty("maxActive","50");
			setMysqlProp.setProperty("maxIdle","50");
			setMysqlProp.setProperty("minIdle","5");
			setMysqlProp.setProperty("maxWait","90000");
			setMysqlProp.setProperty("validationQuery","SELECT 1");
			setMysqlProp.setProperty("testOnBorrow","true");
			setMysqlProp.setProperty("testWhileIdle","true");
			setMysqlProp.setProperty("timeBetweenEvictionRunsMillis","3600000");
			setMysqlProp.setProperty("minEvictableIdleTimeMillis","18000000");
			mysqlds = BasicDataSourceFactory.createDataSource(setMysqlProp);
//供应链
            mysql_jdbcUrl_suppy = prop.getProperty(Constants.MYSQL_JDBC_SUPPLY_URL);
            mysql_jdbcUsername_supply = prop.getProperty(Constants.MYSQL_JDBC_SUPPLY_USERNAME);
            mysql_jdbcPassword_supply = prop.getProperty(Constants.MYSQL_JDBC_SUPPLY_PASSWORD);
            Properties setMysqlSupplyProp = new Properties();
            setMysqlSupplyProp.setProperty("driverClassName",driverClassName);
            setMysqlSupplyProp.setProperty("url",mysql_jdbcUrl_suppy);
            setMysqlSupplyProp.setProperty("username",mysql_jdbcUsername_supply);
            setMysqlSupplyProp.setProperty("password",mysql_jdbcPassword_supply);
            setMysqlSupplyProp.setProperty("maxActive","50");
            setMysqlSupplyProp.setProperty("maxIdle","50");
            setMysqlSupplyProp.setProperty("minIdle","5");
            setMysqlSupplyProp.setProperty("maxWait","90000");
            setMysqlSupplyProp.setProperty("validationQuery","SELECT 1");
            setMysqlSupplyProp.setProperty("testOnBorrow","true");
            setMysqlSupplyProp.setProperty("testWhileIdle","true");
            setMysqlSupplyProp.setProperty("timeBetweenEvictionRunsMillis","3600000");
            setMysqlSupplyProp.setProperty("minEvictableIdleTimeMillis","18000000");
            mysqlds_supply = BasicDataSourceFactory.createDataSource(setMysqlSupplyProp);
//会员
			mysql_jdbcUrl_db_data = prop.getProperty(Constants.MYSQL_JDBC_DB_DATA_URL);
			mysql_jdbcUsername_db_data = prop.getProperty(Constants.MYSQL_JDBC_DB_DATA_USERNAME);
			mysql_jdbcPassword_db_data = prop.getProperty(Constants.MYSQL_JDBC_DB_DATA_PASSWORD);
			Properties setMysqlUdsProp = new Properties();
			setMysqlUdsProp.setProperty("driverClassName",driverClassName);
			setMysqlUdsProp.setProperty("url",mysql_jdbcUrl_db_data);
			setMysqlUdsProp.setProperty("username",mysql_jdbcUsername_db_data);
			setMysqlUdsProp.setProperty("password",mysql_jdbcPassword_db_data);
			setMysqlUdsProp.setProperty("maxActive","50");
			setMysqlUdsProp.setProperty("maxIdle","50");
			setMysqlUdsProp.setProperty("minIdle","5");
			setMysqlUdsProp.setProperty("maxWait","90000");
			setMysqlUdsProp.setProperty("validationQuery","SELECT 1");
			setMysqlUdsProp.setProperty("testOnBorrow","true");
			setMysqlUdsProp.setProperty("testWhileIdle","true");
			setMysqlUdsProp.setProperty("timeBetweenEvictionRunsMillis","3600000");
			setMysqlUdsProp.setProperty("minEvictableIdleTimeMillis","18000000");
			mysqlds_db_data = BasicDataSourceFactory.createDataSource(setMysqlUdsProp);
//db_hedw,元数据库
			db_hedw_jdbcUrl = prop.getProperty(Constants.MYSQL_JDBC_DB_HEDW_JDBCURL_URL);
			db_hedw_jdbcUsername = prop.getProperty(Constants.MYSQL_JDBC_DB_HEDW_JDBCURL_USERNAME);
			db_hedw_jdbcPassword = prop.getProperty(Constants.MYSQL_JDBC_DB_HEDW_JDBCURL_PASSWORD);
			Properties setMysqlHedwProp = new Properties();
			setMysqlHedwProp.setProperty("driverClassName",driverClassName);
			setMysqlHedwProp.setProperty("url",db_hedw_jdbcUrl);
			setMysqlHedwProp.setProperty("username",db_hedw_jdbcUsername);
			setMysqlHedwProp.setProperty("password",db_hedw_jdbcPassword);
			setMysqlHedwProp.setProperty("maxActive","50");
			setMysqlHedwProp.setProperty("maxIdle","50");
			setMysqlHedwProp.setProperty("minIdle","5");
			setMysqlHedwProp.setProperty("maxWait","90000");
			setMysqlHedwProp.setProperty("validationQuery","SELECT 1");
			setMysqlHedwProp.setProperty("testOnBorrow","true");
			setMysqlHedwProp.setProperty("testWhileIdle","true");
			setMysqlHedwProp.setProperty("timeBetweenEvictionRunsMillis","3600000");
			setMysqlHedwProp.setProperty("minEvictableIdleTimeMillis","18000000");
			mysqlds_db_hedw = BasicDataSourceFactory.createDataSource(setMysqlHedwProp);



		} catch (Exception e) {//初始化异常
			throw new ExceptionInInitializerError(e);
		}
	}
	public static DataSource getMtsqlDbHedwSource() {
		return mysqlds_db_hedw;
	}
	public static DataSource getTidbDataSource() {
		return tidbds;
	}
	public static DataSource getMysqlDataSource() {
		return mysqlds;
	}
    public static DataSource getMysqlSupplyDataSource() {
        return mysqlds_supply;
    }
	public static DataSource getMysqlDBDATADataSource() {
		return mysqlds_db_data;
	}
	
	public static Connection getTidbConnection() {
		try {
			return tidbds.getConnection();
		} catch (SQLException e) {
			throw new ExceptionInInitializerError(e);
		}
	}
	public static Connection getMysqlConnection() {
		try {
			return mysqlds.getConnection();
		} catch (SQLException e) {
			throw new ExceptionInInitializerError(e);
		}
	}
	
	
	/**
	 * 我们发现DataSource中没有前面说到的release方法
	 * DataSource获取的Connection都是在本地数据库线程池中的，所有的操作都是同一个线程中来完成的
	 * 也就是说，执行完操作之后，该Connection会自动的被放回本地线程池的
	 * @param con
	 */
	public static void release(Connection con) {
	
	}
}
