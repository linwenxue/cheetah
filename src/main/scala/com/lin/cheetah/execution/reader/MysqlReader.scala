package com.lin.cheetah.execution.reader

import java.util.Properties

import com.lin.cheetah.CheetahConf
import com.lin.cheetah.util.TimeUtils
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.slf4j.LoggerFactory

/**
  * Created by wenxuelin on 2017/6/22.
  */
private[cheetah] class MysqlReader(@transient val sqlContext: SQLContext, val conf: CheetahConf) extends Reader{
  val LOG = LoggerFactory.getLogger("MysqlReader")
  private val url = conf.getDataSourceUrl
  private val driver = conf.getDataSoureDriver
  private val user = conf.getDataSoureUser
  private val password = conf.getDataSourePassword
  private val table = conf.getDataSoureTable
  private val partitionColumn = conf.getPartitionColumn
  private val partitionCondition = conf.getPartitionCondition

  /**
    * 读取mysql数据库数据
    * @return
    */
  override def read(): DataFrame = {
    conf.getExtractType match {
      case conf.EXTRACT_TYPE_FULL => {
        var lowerBound = conf.getColumnLowerBound
        var upperBound = conf.getColumnUpperBound
        val numPartitions = conf.getNumPartitions
        //如有以一个界限值为空值，则读取数据库获取界限值
        if(lowerBound == null || upperBound == null) {
          val minAndMaxValue = getColumnMinAndMaxValue
          lowerBound = minAndMaxValue._1
          upperBound = minAndMaxValue._2
          val partitionSize = upperBound/numPartitions - lowerBound/numPartitions
          lowerBound = lowerBound + partitionSize
          upperBound = upperBound - partitionSize
        }
        val prop = new Properties()
        val dataFrame = sqlContext.read.jdbc(url+"?user="+user+"&password="+password, table, partitionColumn,
          lowerBound, upperBound, numPartitions, prop)
        LOG.warn(s"全量模式=>Mysql表${table}，读取数据量:[${dataFrame.count()}]，数据分区数：$numPartitions")
        dataFrame
      }
      case conf.EXTRACT_TYPE_INCREMENT => {
        val partition = generatePartition()
        val prop = new Properties()
        val dataFrame = sqlContext.read.jdbc(url+"?user="+user+"&password="+password, table, partition, prop)
        LOG.warn(s"增量模式=>Mysql表${table}，源数据分区：${partition.toList}，读取数据量:[${dataFrame.count()}]，数据分区数：${dataFrame.rdd.partitions.length}")
        dataFrame
      }
    }
  }

  /**
    * 读取mysql数据获取table的分区字段的最大和最小边界值
    * @return
    */
  protected def getColumnMinAndMaxValue(): (Long, Long) = {
    val tableDataDF = sqlContext.read.format("jdbc")
                .option("url", url)
                .option("dbtable", table)
                .option("driver", driver)
                .option("user", user)
                .option("password", password)
                .load()
    val tmpTable = table + "_tmp"
    tableDataDF.registerTempTable(tmpTable)
    val querySql = s"SELECT min($partitionColumn), max($partitionColumn) FROM $tmpTable"
    val row =sqlContext.sql(querySql).collect()(0)
    (row.get(0).toString.toLong, row.get(1).toString.toLong)
  }

  /**
    * 增量模式,读取mysql数据，生成partition
    * @return
    */
  def generatePartition(): Array[String] = {
    //如果设置condition自定义条件则优先使用，否则使用contTime时间
    if(partitionCondition.isEmpty) {
      conf.get("countTime").split(",").map {
        time =>
          partitionColumn.split("\\|\\|").map {
            columnAndFormat =>
              val cf = columnAndFormat.split(">")
              val column = cf(0)
              val format = cf(1)
              var startTime: String = null
              var endTime: String = null
              format match {
                case "ts" => {
                  startTime = TimeUtils.timestamp(time+"0000", TimeUtils.DATE_TIME).toString
                  endTime = TimeUtils.timestamp(time+"5959", TimeUtils.DATE_TIME).toString
                }
                case _ => {
                  //时间类型使用单引号''括起来
                  startTime = s"'${TimeUtils.format(time+"0000", TimeUtils.DATE_TIME, format)}'"
                  endTime = s"'${TimeUtils.format(time+"5959", TimeUtils.DATE_TIME, format)}'"
                }
              }
              s"($column>= $startTime AND $column<=$endTime)"
          }.mkString(" OR ")
      }.distinct
    } else {
      partitionCondition.split("\\|\\|")
    }
  }
}
