package hualala.com.utils;
import hualala.com.bean.TableFieldBean;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Creator:Administrator
 * Time:2018-09-30 9:29
 * Project: hiveToEsSpark
 * Description:
 *
 *tinyint,smallint,int,bigint,boolean,float,double,string,binary,timestamp,decimal,char,varchar,DATE
 *
 */
public class SqlUtils {
    private static Map<String,String> hiveMap=new HashMap<>();
    static {
        //初始化hive和tidb类型对照表,key为hive数据类型，值为tidb数据类型
        hiveMap.put("tinyint","tinyint");
        hiveMap.put("smallint","smallint");
        hiveMap.put("int","int");
        hiveMap.put("bigint","bigint");
        hiveMap.put("boolean","boolean");
        hiveMap.put("float","float");
        hiveMap.put("double","double");
        hiveMap.put("string","varchar(200)");
        hiveMap.put("binary","binary");
        hiveMap.put("timestamp","timestamp(1)");
        hiveMap.put("decimal","decimal(15,2)");
        hiveMap.put("char","char");
        hiveMap.put("varchar","varchar(254)");
        hiveMap.put("date","date");
        hiveMap.put("integer","int");
        hiveMap.put("long","bigint");
    }
    public static String initCreateTableSql(List<TableFieldBean> list, String tableName,String isPrimaryKey,String primaryKeyName) {
        if(list==null || list.size() <=0 || tableName==null || tableName.equals("")){
            return "";
        }
        String fieldS="";
        StringBuilder sb=new StringBuilder("CREATE TABLE "+tableName +" (").append("\n");
        int length=list.size();
        TableFieldBean tbf=null;
        for (int i=0;i<length;i++){
            tbf=list.get(i);
            if(i==length-1){
                if(isPrimaryKey!=null&&isPrimaryKey.equals("0")){
                    fieldS=enclosureTidbField(tbf,isPrimaryKey,primaryKeyName);
                    sb.append(fieldS).append(",").append("\n");
                }else{
                    fieldS=enclosureTidbField(tbf,isPrimaryKey,primaryKeyName);
                    sb.append(fieldS).append("\n");
                }
            }else{
                fieldS=enclosureTidbField(tbf,isPrimaryKey,primaryKeyName);
                sb.append(fieldS).append(",").append("\n");
            }
        }
       if(isPrimaryKey.equals("0")){
           sb.append("PRIMARY KEY ("+primaryKeyName+")").append("\n");
        }
        sb.append(") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin");
        list=null;
        return  sb.toString();
    }
     private static String enclosureTidbField(TableFieldBean tbf,String isPrimaryKey,String primaryKeyName) {
         if(isPrimaryKey!=null&&isPrimaryKey.equals("0")&& primaryKeyName.equals(tbf.getFieldName())){
             return tbf.getFieldName()+" "+hiveMap.get(tbf.getFieldType())+" NOT NULL COMMENT " +"\""+tbf.getFieldComment()+"\"";
         }else {
             return tbf.getFieldName() + " " + hiveMap.get(tbf.getFieldType()) + " DEFAULT NULL COMMENT " + "\"" + tbf.getFieldComment() + "\"";
         }
    }

    public static String initInsertSql(String fields, String tableName) {
        StringBuilder sb=new StringBuilder("replace into "+tableName +" ( ");
        sb.append(fields).append(" ) VALUES ( ");
        Integer len=fields.split(",").length;
        for(int i=0;i<len;i++){
            if(i==len-1){
                sb.append("?");
            }else{
                sb.append("?").append(",");
            }
         }
         sb.append(" )");
        System.out.println(sb.toString());
        return sb.toString();
    }
    //给内部线用的临时方案
    public static String initInsertSqlTemp(String fields, String tableName) {
        StringBuilder sbTest = new StringBuilder("replace into "+tableName +" ( ");
        sbTest.append("dateDate,groupID, shopID, settleUnitID, createTime, transType, transAmount, transSettleRate, orderID, orderKey, cardID, trdPayWayCode, trdMyOrderNo, trdTransAmount").append(" ) VALUES ( ");
        Integer len=fields.split(",").length;
        for(int i=0;i<len;i++){
            if(i==len-1){
                sbTest.append("?");
            }else{
                sbTest.append("?").append(",");
            }
        }
        sbTest.append(" )");
        return sbTest.toString();
    }

    public static String initInsertSqlTwo(String fields, String tableName) {
        StringBuilder sb=new StringBuilder("insert into "+tableName +" ( ");
        sb.append(fields).append(" ) VALUES ( ");
        Integer len=fields.split(",").length;
        for(int i=0;i<len;i++){
            if(i==len-1){
                sb.append("?");
            }else{
                sb.append("?").append(",");
            }
        }
        sb.append(" )");
        return sb.toString();
    }

    public static String initInsertSqlMysql(String fields, String tableName) {
        StringBuilder sb=new StringBuilder("insert into "+tableName +" ( ");
        StringBuilder sbu=new StringBuilder();
        sb.append(fields).append(" ) VALUES ( ");
        String[] fieldArr=fields.split(",");
        Integer len=fieldArr.length;
        for(int i=0;i<len;i++){
            if(i==len-1){
                sbu.append(fieldArr[i]).append("=VALUES(").append(fieldArr[i]).append(")");
                sb.append("?");
            }else{
                sb.append("?").append(",");
                sbu.append(fieldArr[i]).append("=VALUES(").append(fieldArr[i]).append(")").append(",");
            }
        }
        sb.append(" )").append(" ON DUPLICATE KEY UPDATE ").append(sbu.toString());
        return sb.toString();
    }
}
