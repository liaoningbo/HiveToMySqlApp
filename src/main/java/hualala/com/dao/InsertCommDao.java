package hualala.com.dao;
import hualala.com.bean.CommInsertBean;
import hualala.com.bean.TableFieldBean;

import java.util.List;
/**
 * Creator:Administrator
 * Time:2018-09-29 14:21
 * Project: hiveToEsSpark
 * Description:
 */
public interface InsertCommDao {
     void batch(List<CommInsertBean> insertList,String fields,String tableName ,String type);
     void insert(CommInsertBean bean);
     boolean isExistTable(String tableName);
     void batchUpdate(List<CommInsertBean> updateList);
     void update(CommInsertBean bean);
     int delete(String bean);
     boolean createTable(List<TableFieldBean> list,String tableName,String isPrimaryKey,String primaryKeyName);
     boolean tracateTable(String tableName);
     int batchDeleteByInConditionAndPrimaryKey(String tableName, String field, List<Object> conditions);
}
