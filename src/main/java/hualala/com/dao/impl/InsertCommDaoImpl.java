package hualala.com.dao.impl;

import hualala.com.bean.CommInsertBean;
import hualala.com.bean.TableFieldBean;
import hualala.com.bean.TableNameBean;
import hualala.com.dao.InsertCommDao;
import hualala.com.utils.Constants;
import hualala.com.utils.DBCPUtil;
import hualala.com.utils.SqlUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanHandler;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
/**
 * Creator:Administrator
 * Time:2018-09-29 14:41
 * Project: hiveToEsSpark
 * Description:
 */
public class InsertCommDaoImpl implements InsertCommDao {

    private QueryRunner qr = null;
    private String existSql="SHOW TABLES LIKE ";
    private String tracateSql="TRUNCATE TABLE ";
    @Override
    public void batch(List<CommInsertBean> insertList, String fields, String tableName, String type) {
        //封装执行的insert语句
        String insertSql="";
        if(type.equals(Constants.INSERT_SQL)){
            insertSql=SqlUtils.initInsertSqlTwo(fields,tableName);
        }else if(type.equals(Constants.REPLACE_SQL)){
            insertSql=SqlUtils.initInsertSql(fields,tableName);
        }else if(type.equals(Constants.DUPLICATE_SQL)){
            insertSql=SqlUtils.initInsertSqlMysql(fields,tableName);
        }
        Object[][] params = new Object[insertList.size()][];
        for (int i = 0; i < insertList.size(); i++) {
            CommInsertBean ab = insertList.get(i);
            params[i] = ab.getObj();
        }
        try {
            qr.batch(insertSql, params);
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
    @Override
    public void insert(CommInsertBean bean) {


    }
    public InsertCommDaoImpl(String dsTag) {
        if(dsTag!=null && !dsTag.isEmpty()){
             if(dsTag.equals("tidb")){
                 qr = new QueryRunner(DBCPUtil.getTidbDataSource());
             }else if(dsTag.equals("mysql")){
                 qr = new QueryRunner(DBCPUtil.getMysqlDataSource());
             }else if(dsTag.equals("mysql_supply")){
                qr = new QueryRunner(DBCPUtil.getMysqlSupplyDataSource());
            }else if(dsTag.equals("mysql_db_data")){
                 qr = new QueryRunner(DBCPUtil.getMysqlDBDATADataSource());
             }else if(dsTag.equals("mysql_db_hedw")){
                 qr = new QueryRunner(DBCPUtil.getMtsqlDbHedwSource());
             }else{
                 qr = new QueryRunner(DBCPUtil.getTidbDataSource());
             }
        }else{
                 qr = new QueryRunner(DBCPUtil.getTidbDataSource());
        }
    }

    public InsertCommDaoImpl() {
    }

    @Override
    public boolean isExistTable(String tableName) {
        existSql=existSql+"\""+tableName+"\"";
        try {
            TableNameBean bean=qr.query(existSql,new BeanHandler<TableNameBean>(TableNameBean.class));
            if(bean==null){
                return  false;
            }else{
                return  true;
            }
       } catch (SQLException e) {
            e.printStackTrace();
        }
        return true;
    }


    @Override
    public void batchUpdate(List<CommInsertBean> updateList) {

    }

    /**
     *
     * @param tableName    表名
     * @param field        字段域
     * @param conditions   条件：格式：(id1,id2)
     * @return
     */
    @Override
    public int batchDeleteByInConditionAndPrimaryKey(String tableName, String field, List<Object> conditions) {
        if (conditions != null && !conditions.isEmpty()) {
            String preSql = " DELETE  FROM " + tableName + " WHERE " + field + " IN ";
            StringBuilder c = new StringBuilder(preSql);
            Object[] object=new Object[conditions.size()];
            Integer i=0;
            c.append("(");
            for (Object condition : conditions) {
                c.append("?").append(",");
                object[i]=condition;
                i++;
            }
            c.deleteCharAt(c.length() - 1);
            c.append(")");
            System.out.println(c.toString());
            try {
                int rel=qr.update(c.toString(),object);
                if(rel>=0){
                    return  rel;
                }else{
                    return  0;
                }
            } catch (SQLException e) {
                e.printStackTrace();
                return  0;
            }
        }
        return  0;
    }


    @Override
    public void update(CommInsertBean bean) {

    }

    @Override
    public int delete(String sql) {
        try {
            return  qr.update(sql);
        }catch (SQLException e){
                 e.printStackTrace();
                 return  0;
           }
    }

    @Override
    public boolean createTable(List<TableFieldBean> list,String tableName,String isPrimaryKey,String primaryKeyName){
     String createTableSql= SqlUtils.initCreateTableSql(list,tableName,isPrimaryKey,primaryKeyName);
        System.out.print(createTableSql);
        Connection conn = null;
        Statement stmt = null;
        try {
           // TableNameBean bean=qr.query(createTableSql,new BeanHandler<TableNameBean>(TableNameBean.class));
            conn=qr.getDataSource().getConnection();
            stmt=conn.createStatement();
            int tt=stmt.executeUpdate(createTableSql);
            if(tt==0){
                return true;
            }else{
                return  false;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }finally{
            //finally block used to close resources
            try{
                if(stmt!=null) {
                    conn.close();
                }
            }catch(SQLException se){
            }// do nothing
            try{
                if(conn!=null) {
                    conn.close();
                }
            }catch(SQLException se){
                se.printStackTrace();
            }//end finally try
        }//end try
    }
    @Override
    public boolean tracateTable(String tableName) {
        System.out.print("begin tracate table-------------------------");
        Connection conn = null;
        Statement stmt = null;
        try {
            conn=qr.getDataSource().getConnection();
            stmt=conn.createStatement();
            int tt=stmt.executeUpdate(tracateSql+tableName);
            if(tt==0){
                return true;
            }else{
                return  false;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }finally{
            //finally block used to close resources
            try{
                if(stmt!=null) {
                    conn.close();
                }
            }catch(SQLException se){
            }// do nothing
            try{
                if(conn!=null) {
                    conn.close();
                }
            }catch(SQLException se){
                se.printStackTrace();
            }//end finally try
        }//end try
    }
}
