package hualala.com.main

import java.text.SimpleDateFormat
import java.util
import java.util.Properties

import hualala.com.bean.{CommInsertBean, HiveSchema, JdbcConnectBean, TableFieldBean}
import hualala.com.dao.impl.InsertCommDaoImpl
import hualala.com.utils.{Constants, DBCPUtil}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.util.LongAccumulator
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
/**
  * Creator:zhangpeng
  * Time:2018-09-17 19:40
  * Project: hiveToEsSpark
  * Description: 将hive中的数据同步至Es的spark程序
  *
  */
class HiveToMySqlApp extends Serializable {
  private val dsdf: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
  val logger = LoggerFactory.getLogger(HiveToMySqlApp.getClass)
  private val appname: String = "HiveToMySqlApp"

  def enclosureDataToBean(e: Row, fields: String): CommInsertBean = {
    val aFields = fields.split(",")
    val bean = new CommInsertBean()
    val obj = new Array[Object](e.size)
    for (a <- 0 until aFields.size) {
      obj(a) = e.getAs[Object](aFields(a))

    }
    bean.setObj(obj)
    bean
  }

  def OperationTidb(iter: Iterator[Row], table: String, fields: String, primaryKeyName: String, sourceDb: String, tag: String, longAccumulator: LongAccumulator): Unit = {
    var list = new util.ArrayList[CommInsertBean](2500)
    val tableService = new InsertCommDaoImpl(sourceDb)
    var count = 0
    for (e <- iter) {
      //将数据封装到一个list中并插入到数据库
      val rel = enclosureDataToBean(e, fields)
      list.add(rel)
      count += 1
      longAccumulator.add(1)
      if (count % 2500 == 0) {
        logger.info("--插入数据的大小---" + list.size())
        tableService.batch(list, fields, table, tag)
        count = 0
        list = new util.ArrayList[CommInsertBean](2500)
      }
    }
    logger.info("--插入数据的大小---" + list.size())
    tableService.batch(list, fields, table, tag)
  }

  //删除操作
  def deleteTidbOperation(iter: Iterator[Row], tableName: String, primaryKey: String, sourceDb: String, longAccumulator: LongAccumulator): Unit = {
    var list = new util.ArrayList[Object](2500)
    val tableHandler = new InsertCommDaoImpl(sourceDb)
    var count = 0
    for (e <- iter) {
      list.add(e.getAs[Object](primaryKey))
      count += 1
      if (count % 2500 == 0) {
        val res = tableHandler.batchDeleteByInConditionAndPrimaryKey(tableName, primaryKey, list)
        count = 0
        list = new util.ArrayList[Object](2500)
        longAccumulator.add(res)
      }
    }
    val res = tableHandler.batchDeleteByInConditionAndPrimaryKey(tableName, primaryKey, list)
    longAccumulator.add(res)
  }

  def interceptTypeName(typeName: String): String = {
    if (typeName.contains("(") || typeName.contains(")")) {
      typeName.split("\\(")(0)
    } else {
      typeName
    }
  }

  def getComment(sc: StructField): String = {
    val ssc = sc.getComment()
    if (ssc == null || ssc.isEmpty) {
      ""
    } else {
      ssc.get.toString
    }
  }

  def InitTidbTable(schema: StructType, tableName: String, isPrimaryKey: String, primaryKeyName: String, sourceDb: String): String = {
    //判断表是否存在，如果表已经存在，则跳过这个步骤
    val list = new util.ArrayList[TableFieldBean]()
    val tableService = new InsertCommDaoImpl(sourceDb)
    val schemas: Iterator[StructField] = schema.toIterator
    val rel = tableService.isExistTable(tableName)
    val fileds = new StringBuilder()
    for (sc <- schemas) {
      val bean = new TableFieldBean()
      bean.setFieldName(sc.name)
      //对带有长度定义的数据类型进行截取
      val typeName = interceptTypeName(sc.dataType.typeName)
      bean.setFieldType(typeName)
      bean.setFieldComment(getComment(sc))
      bean.setNullable(sc.nullable)
      fileds.append(sc.name).append(",")
      list.add(bean)
    }
    if (!rel) {
      val crel = tableService.createTable(list, tableName, isPrimaryKey, primaryKeyName)
      if (!crel) {
        throw new Exception("创建表失败了，请检查")
      }
    }
    val ret = fileds.toString().substring(0, fileds.length - 1)
    ret
  }

  def gainConfig(sourceDb: String): JdbcConnectBean = {
    val conf = new JdbcConnectBean()
    if (sourceDb != null && !sourceDb.isEmpty) {
      if (sourceDb.equals("tidb")) {
        conf.setJdbcUrl(DBCPUtil.tidb_jdbcUrl)
        conf.setJdbcUsername(DBCPUtil.tidb_jdbcUsername)
        conf.setJdbcPassword(DBCPUtil.tidb_jdbcPassword)
      } else if (sourceDb.equals("mysql")) {
        conf.setJdbcUrl(DBCPUtil.mysql_jdbcUrl)
        conf.setJdbcUsername(DBCPUtil.mysql_jdbcUsername)
        conf.setJdbcPassword(DBCPUtil.mysql_jdbcPassword)
      } else {
        conf.setJdbcUrl(DBCPUtil.tidb_jdbcUrl)
        conf.setJdbcUsername(DBCPUtil.tidb_jdbcUsername)
        conf.setJdbcPassword(DBCPUtil.tidb_jdbcPassword)
      }
    } else {
      conf.setJdbcUrl(DBCPUtil.tidb_jdbcUrl)
      conf.setJdbcUsername(DBCPUtil.tidb_jdbcUsername)
      conf.setJdbcPassword(DBCPUtil.tidb_jdbcPassword)
    }
    conf
  }

  def truncateTable(tableName: String, sourceDb: String): Boolean = {
    val tableService = new InsertCommDaoImpl(sourceDb)
    tableService.tracateTable(tableName)
  }

  def deleteMysqlOrTidbDataOperation(tableName: String, sourceDb: String, sourceDelSql: String) = {
    val tableService = new InsertCommDaoImpl(sourceDb)
    val deleteSql=sourceDelSql+" limit 10000"
    var i=1
    while(i>0){
     i=tableService.delete(deleteSql)
    }
  }

  def appStart(sql: String, delSql: String, warehouse: String, tableName: String, warehouseDir: String, isPrimaryKey: String, primaryKeyName: String, isDeteleDate: String, repartitionNum: Int, sourceDb: String, sourceDelSql: String): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project.jetty.server").setLevel(Level.WARN)

    logger.info("sql----------" + sql + "--------------------------------------")
    logger.info("delSql----------" + delSql + "--------------------------------------")
    import org.apache.spark.sql.SparkSession
    val sqlContext = SparkSession.builder().appName(appname).enableHiveSupport().config("spark.sql.warehouse.dir", warehouseDir).config("spark.shuffle.consolidateFiles", "true").getOrCreate()
    sqlContext.sql("use " + warehouse)
    val dataDF = sqlContext.sql(sql)
    logger.info("需要插入的数据量:-----------------" + dataDF.count())
    //判断表是否存在并根据规则智能生成建表语句
    val accumulatorDelete = sqlContext.sparkContext.longAccumulator("delete")
    val accumulatorInsert = sqlContext.sparkContext.longAccumulator("insert")
    val fileds = InitTidbTable(dataDF.schema, tableName, isPrimaryKey, primaryKeyName, sourceDb)
    //对数据进行重新分区
    val dataFrameDate = dataDF.repartition(repartitionNum)
    logger.info("字段信息值-----------------------" + fileds)
    //获取配置信息
    val config = gainConfig(sourceDb)
    val prop = new Properties()
    prop.put("user", config.getJdbcUsername)
    prop.put("password", config.getJdbcPassword)
    if (isDeteleDate != null){
    //是否需要按照特定的条件删除mysql，或者tidb中的数据，清空历史数据
      if (sourceDelSql != null && !sourceDelSql.isEmpty) {
        logger.info("-----------------start delete history Data-----------------")
        deleteMysqlOrTidbDataOperation(tableName, sourceDb, sourceDelSql)
      }
    isDeteleDate match {
      case "0" =>
        logger.info("-----------------start truncate-----------------")
        val rel = truncateTable(tableName, sourceDb)
        if (rel) {
          logger.info("----------start replace into-------------")
          dataFrameDate.foreachPartition(row => OperationTidb(row, tableName, fileds, primaryKeyName, sourceDb, Constants.INSERT_SQL, accumulatorInsert))
        }
      case "1" =>
        logger.info("----------append----------")
        dataFrameDate.write.mode(SaveMode.Append).jdbc(config.getJdbcUrl, tableName, prop)
      case "2" => //根据条件删除
        if (delSql != null && !delSql.equals("") && isPrimaryKey != null && isPrimaryKey.equals("0")) {
          val delDataFrame = sqlContext.sql(delSql)
          logger.info("-----------------delete count-------------" + delDataFrame.count())
          logger.info("-----------------start delete-----------------")
          delDataFrame.foreachPartition(row => deleteTidbOperation(row, tableName, primaryKeyName, sourceDb, accumulatorDelete))
          logger.info("-----------------start replace into-----------------")
          dataFrameDate.foreachPartition(row => OperationTidb(row, tableName, fileds, primaryKeyName, sourceDb, Constants.REPLACE_SQL, accumulatorInsert))
        } else {
          logger.info("delete param is not ok")
        }
      case "3" =>
        if (isPrimaryKey != null && !isPrimaryKey.equals("")) {
          //将数据分批插入到tidb中,可以控制插入的条数，重新分区，增加执行的tasks
          logger.info("-----------------start replace into-----------------")
          dataFrameDate.foreachPartition(row => OperationTidb(row, tableName, fileds, primaryKeyName, sourceDb, Constants.REPLACE_SQL, accumulatorInsert))
        } else {
          logger.error("replace into param is no ok")
        }
      case "4" =>
        if (isPrimaryKey != null && !isPrimaryKey.equals("")) {
          //将数据分批插入到tidb中,可以控制插入的条数，重新分区，增加执行的tasks
          logger.info("-----------------start insert into-----------------")
          dataFrameDate.foreachPartition(row => OperationTidb(row, tableName, fileds, primaryKeyName, sourceDb, Constants.INSERT_SQL, accumulatorInsert))
        } else {
          logger.error("insert into param is no ok")
        }
      case "5" =>
        if (delSql != null && !delSql.equals("") && isPrimaryKey != null && isPrimaryKey.equals("0")) {
          val delDataFrame = sqlContext.sql(delSql)
          logger.info("-----------------delete count-------------" + delDataFrame.count())
          logger.info("-----------------start delete-----------------")
          delDataFrame.foreachPartition(row => deleteTidbOperation(row, tableName, primaryKeyName, sourceDb, accumulatorDelete))
          logger.info("-----------------start replace into-----------------")
          dataFrameDate.foreachPartition(row => OperationTidb(row, tableName, fileds, primaryKeyName, sourceDb, Constants.DUPLICATE_SQL, accumulatorInsert))
        } else {
          logger.info("delete param is not ok")
        }
      case _ =>
        logger.error(" param isDeteleDate is not ok")
    }
  }

  logger.info(accumulatorInsert.name.get + "插入了：" + accumulatorInsert.value + "条数据")
  logger.info(accumulatorDelete.name.get + "删除了：" + accumulatorDelete.value + "条数据")
  sqlContext.stop()
  }
}
object  HiveToMySqlApp{
  val logger = LoggerFactory.getLogger(HiveToMySqlApp.getClass)
  /**
    *
    * @param args
    */
  def main(args: Array[String]) {
    if (args.length < 11) {
      println("Usage: <warehouse> <tableName> <warehouseDir><isPrimaryKey> <primaryKeyName><idDeteleDate><repartitionNum><sql><delSql><isDeleteBySql>")
      sys.exit(-1)
    }
    val warehouse:String=args(0)
    val tableName:String=args(1)
    val warehouseDir:String=args(2)
    val isPrimaryKey:String=args(3)
    val primaryKeyName:String=args(4)
    val idDeteleDate:String=args(5)
    val repartitionNum:Int=Integer.valueOf(args(6))
    val sourceDb:String=args(7)
    val sql:String =args(8)
    val delSql:String  = args(9)
    val sourceDelSql:String=args(10)
    new HiveToMySqlApp().appStart(sql,delSql,warehouse,tableName,warehouseDir,isPrimaryKey,primaryKeyName,idDeteleDate,repartitionNum,sourceDb,sourceDelSql)
  }
}
