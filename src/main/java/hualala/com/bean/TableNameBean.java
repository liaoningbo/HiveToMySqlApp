package hualala.com.bean;

import java.io.Serializable;

/**
 * Creator:Administrator
 * Time:2018-09-29 19:38
 * Project: hiveToEsSpark
 * Description:
 */
public class TableNameBean implements Serializable{
    private  String tableName;

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }
}
