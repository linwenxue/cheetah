package com.lin.cheetah.execution

import com.lin.cheetah.CheetahConf
import com.lin.cheetah.util.TimeUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

/**
  * Created by wenxuelin on 2017/6/23.
  */
private[cheetah] class HiveWriter(@transient val sparkContext: SparkContext, val conf: CheetahConf) extends Writer{
  val LOG = LoggerFactory.getLogger("HiveWriter")

  override def write(inputDataFrame: DataFrame): Unit = {
    val hiveContext: HiveContext = new HiveContext(sparkContext)
    hiveContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    val table = conf.getHiveOutputTable
    val tableColumns = hiveContext.sql(s"DESC ${table}")
    val columns = tableColumns.collect().toList
    val tableSchema = StructType(columns.map(column =>
      StructField(column(0).toString,
        column(1).toString match {
          case "string" => StringType
          case "int" => IntegerType
          case "long" => LongType
          case "double" => DoubleType
          /*
            读取mysql的float类型spark-sql会默认转化为double,
            所有最好hive中使用double替换float,否则存在double值精度丢失
           */
          case "float" => DoubleType
          case "tinyint" => IntegerType
          case "bigint" => LongType
        }, true)))

    //缓存df
    inputDataFrame.persist(StorageLevel.MEMORY_AND_DISK)
    //根据抽取方式判断是否写入hive分区表
    def writeByMode(): Unit = {
      conf.getExtractType match {
        case conf.EXTRACT_TYPE_FULL => {
          val dataFrame = hiveContext.createDataFrame(inputDataFrame.rdd, tableSchema)
          val writer = dataFrame.write.format("parquet").mode(SaveMode.Overwrite)
          writer.insertInto(s"${table}")
          LOG.warn(s"全量模式=>插入Hive表${table}数据量:[${dataFrame.count()}]]")
        }
        case conf.EXTRACT_TYPE_INCREMENT => {
          val partitionColumns = conf.getPartitionColumn
          def partition(rows: Iterator[Row]): Iterator[Row] = {
            rows.map {
              row =>
                val time = partitionColumns.split("\\|\\|").map {
                  columnAndFormat =>
                    val cf = columnAndFormat.split(">")
                    TimeUtils.format(row.getAs[String](cf(0)), cf(1), TimeUtils.DATE)
                }.filter(x => x != null)
                Row((row.toSeq :+ time(time.length-1).toInt): _*)
            }
          }
          val dataFrame = hiveContext.createDataFrame(inputDataFrame.mapPartitions(partition), tableSchema)
          val writer = dataFrame.write.format("parquet").mode(SaveMode.Append)
          writer.partitionBy("dt")
          writer.insertInto(s"${table}")
          LOG.warn(s"增量模式=>插入Hive表${table}数据量:[${dataFrame.count()}]]")
        }
      }
    }
    writeByMode()
  }
}
