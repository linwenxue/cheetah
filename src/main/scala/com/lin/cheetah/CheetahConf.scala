package com.lin.cheetah

import java.io.FileInputStream
import java.util.Properties

import org.apache.commons.lang3.StringUtils

import scala.collection.JavaConverters._
/**
  * Created by wenxuelin on 2017/6/23.
  */
private[cheetah] class CheetahConf extends Serializable{
  private val DS_URL = "datasource.url"
  private val DS_USER = "datasource.user"
  private val DS_PASSWORD = "datasource.password"
  private val DS_DRIVER = "datasource.driver-class-name"
  private val DS_TABLE = "datasource.table"
  private val DS_ET_TYPE = "datasource.extract.type"
  private val DS_PT_COLUMN = "datasource.partition.column"
  private val DS_PT_CONDITION = "datasource.partition.condition"
  private val DS_OT_COLUMN_LOWERBOUND = "datasource.partition.column.lowerBound"
  private val DS_PT_COLUMN_UPPERBOUND = "datasource.partition.column.upperBound"
  private val DS_PT_COLUMN_NUMPARTITIONS = "datasource.partition.column.numPartitions"
  private val HIVE_OUTPUT_TABLE = "hive.output.table"
  private val DS_READER_CLAZZ = "datasource.reader.class"
  private val DS_WRITER_CLAZZ = "datasource.writer.class"
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

  private def init(): CheetahConf = {
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

  def getDataSourceUrl: String = get(DS_URL)

  def getDataSoureUser: String = get(DS_USER)

  def getDataSourePassword: String = get(DS_PASSWORD)

  def getDataSoureDriver: String = get(DS_DRIVER)

  def getDataSoureTable: String = get(DS_TABLE)

  def getExtractType: String = get(DS_ET_TYPE)

  def getPartitionColumn: String = get(DS_PT_COLUMN)

  def getPartitionCondition: String = get(DS_PT_CONDITION)

  def getColumnLowerBound: java.lang.Long = {
    val lower = get(DS_OT_COLUMN_LOWERBOUND)
    if(lower != null && !"".equals(lower)) {
      lower.toLong
    } else {
      null
    }
  }

  def getColumnUpperBound: java.lang.Long = {
    val upper = get(DS_PT_COLUMN_UPPERBOUND)
    StringUtils.isNotBlank(upper) match {
      case true => upper.toLong
      case _ => null
    }
  }

  def getNumPartitions: java.lang.Integer = {
    val numPartition = get(DS_PT_COLUMN_NUMPARTITIONS)
    if(numPartition != null && !"".equals(numPartition)) {
      numPartition.toInt
    } else {
      null
    }
  }

  def getHiveOutputTable: String = get(HIVE_OUTPUT_TABLE)

  def getReaderClassString(): Option[String] = Some(get(DS_READER_CLAZZ))

  def getWriterClassString(): Option[String] = Some(get(DS_WRITER_CLAZZ))
}
