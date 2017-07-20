package com.lin.cheetah

import java.io.FileInputStream
import java.util.Properties

import scala.collection.JavaConverters._
/**
  * Created by wenxuelin on 2017/6/23.
  */
private[cheetah] class CheetahConf extends Serializable{
  private val DATASOURCE_URL = "datasource.url"
  private val DATASOURCE_USER = "datasource.user"
  private val DATASOURCE_PASSWORD = "datasource.password"
  private val DATASOURCE_DRIVER = "datasource.driver-class-name"
  private val DATASOURCE_TABLE = "datasource.table"
  private val DATASOURCE_EXTRACT_TYPE = "datasource.extract.type"
  private val DATASOURCE_PARTITION_COLUMN = "datasource.partition.column"
  private val DATASOURCE_PARTITION_CONDITION = "datasource.partition.condition"
  private val DATASOURCE_PARTITION_COLUMN_LOWERBOUND = "datasource.partition.column.lowerBound"
  private val DATASOURCE_PARTITION_COLUMN_UPPERBOUND = "datasource.partition.column.upperBound"
  private val DATASOURCE_PARTITION_COLUMN_NUMPARTITIONS = "datasource.partition.column.numPartitions"
  private val HIVE_OUTPUT_TABLE = "hive.output.table"
  private[cheetah] val EXTRACT_TYPE_FULL = "0" //全量
  private[cheetah] val EXTRACT_TYPE_INCREMENT = "1" //增量
  private val conf = new scala.collection.mutable.HashMap[String, String]

  private[cheetah] def init(path: String): CheetahConf = {
    if(path == null || "".equals(path.trim)) {
      throw new IllegalArgumentException("path参数不能为空")
    }

    var inputStream: FileInputStream = null
    try {
      val properties = new Properties()
      inputStream  = new FileInputStream(path)
      properties.load(inputStream)
      setConf(properties)
    } catch {
      case e : Exception => throw new RuntimeException(s"初始化${path}配置文件错误:${e.getMessage}")
    } finally {
      if(inputStream != null)
        inputStream.close()
    }
    this
  }

  def init(): CheetahConf = {
    var inputStream: FileInputStream = null
    try {
      val properties = new Properties()
      val path = Thread.currentThread().getContextClassLoader.getResource("conf.properties").getPath
      inputStream = new FileInputStream(path)
      properties.load(inputStream)
      setConf(properties)
    } catch {
      case e : Exception => throw new RuntimeException(s"初始化conf.properties配置文件错误:${e.getMessage}")
    } finally {
      if(inputStream != null)
        inputStream.close()
    }
    this
  }

  def setConf(props: Properties): Unit = {
    props.asScala.foreach { case (k, v) => setConfString(k, v)}
  }

  def setConfString(key: String, value: String): Unit = {
    conf(key) = value
  }

  def get(key: String): String = {
    conf.getOrElse(key, null)
  }

  def getDataSourceUrl: String = get(DATASOURCE_URL)

  def getDataSoureUser: String = get(DATASOURCE_USER)

  def getDataSourePassword: String = get(DATASOURCE_PASSWORD)

  def getDataSoureDriver: String = get(DATASOURCE_DRIVER)

  def getDataSoureTable: String = get(DATASOURCE_TABLE)

  def getExtractType: String = get(DATASOURCE_EXTRACT_TYPE)

  def getPartitionColumn: String = get(DATASOURCE_PARTITION_COLUMN)

  def getPartitionCondition: String = get(DATASOURCE_PARTITION_CONDITION)

  def getColumnLowerBound: java.lang.Long = {
    val lower = get(DATASOURCE_PARTITION_COLUMN_LOWERBOUND)
    if(lower != null && !"".equals(lower)) {
      lower.toLong
    } else {
      null
    }
  }

  def getColumnUpperBound: java.lang.Long = {
    val upper = get(DATASOURCE_PARTITION_COLUMN_UPPERBOUND)
    if(upper != null && !"".equals(upper)) {
      upper.toLong
    } else {
      null
    }
  }

  def getNumPartitions: java.lang.Integer = {
    val numPartition = get(DATASOURCE_PARTITION_COLUMN_NUMPARTITIONS)
    if(numPartition != null && !"".equals(numPartition)) {
      numPartition.toInt
    } else {
      null
    }
  }

  def getHiveOutputTable: String = get(HIVE_OUTPUT_TABLE)
}
