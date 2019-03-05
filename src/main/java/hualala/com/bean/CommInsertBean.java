package hualala.com.bean;

/**
 * Creator:Administrator
 * Time:2018-09-29 14:43
 * Project: hiveToEsSpark
 * Description:
 */
public class CommInsertBean {

    private  Object[] obj;

    private String deleteSql;
    private int size;

    public void setSize(int size) {
        this.size = size;
    }
    public int getSize() {
        return size;
    }
    public void setDeleteSql(String deleteSql) {
        this.deleteSql = deleteSql;
    }
    public String getDeleteSql() {
        return deleteSql;
    }
    public void setObj(Object[] obj) {
        this.obj = obj;
    }

    public Object[] getObj() {
        return obj;
    }
}
