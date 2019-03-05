package hualala.com.utils;
public interface Constants {
    /**
     * spark任务运行的模式，分为本地开发环境，测试环境，生产环境
     */
    String SPARK_JOB_RUN_MODE = "job.run.mode";
    /**
     * 关于JDBC的配置信息
     */
    String JDBC_DRIVER_CLASSNAME = "driverClassName";
    String TIDB_JDBC_URL = "tidb_url";
    String TIDB_JDBC_USERNAME = "tidb_username";
    String TIDB_JDBC_PASSWORD = "tidb_password";
    String MYSQL_JDBC_URL = "mysql_url";
    String MYSQL_JDBC_USERNAME = "mysql_username";
    String MYSQL_JDBC_PASSWORD = "mysql_password";
    String MYSQL_JDBC_SUPPLY_URL = "mysql_url_supply";
    String MYSQL_JDBC_SUPPLY_USERNAME = "mysql_username_supply";
    String MYSQL_JDBC_SUPPLY_PASSWORD = "mysql_password_supply";


    String MYSQL_JDBC_DB_HEDW_JDBCURL_URL = "mysql_db_hedw_url";
    String MYSQL_JDBC_DB_HEDW_JDBCURL_USERNAME = "mysql_db_hedw_username";
    String MYSQL_JDBC_DB_HEDW_JDBCURL_PASSWORD = "mysql_db_hedw_password";

    String MYSQL_JDBC_DB_DATA_URL = "mysql_url_db_data";
    String MYSQL_JDBC_DB_DATA_USERNAME = "mysql_username_db_data";
    String MYSQL_JDBC_DB_DATA_PASSWORD = "mysql_password_db_data";

    String INSERT_SQL="insert";
    String REPLACE_SQL="replace";
    String DUPLICATE_SQL="duplicate";
}
