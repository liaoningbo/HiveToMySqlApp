package hualala.com.bean;

import java.io.Serializable;

/**
 * Creator:张鹏
 * Time:2018-10-09 16:05
 * Project: HiveToMySqlApp
 * Description:
 */
public class JdbcConnectBean implements Serializable {
    private  String jdbcUrl;
    private String jdbcUsername;
    private String jdbcPassword;

    public String getJdbcPassword() {
        return jdbcPassword;
    }

    public void setJdbcPassword(String jdbcPassword) {
        this.jdbcPassword = jdbcPassword;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public String getJdbcUsername() {
        return jdbcUsername;
    }

    public void setJdbcUsername(String jdbcUsername) {
        this.jdbcUsername = jdbcUsername;
    }
}
