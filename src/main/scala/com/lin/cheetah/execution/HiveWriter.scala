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
           * 读取mysql的float类型spark-sql会默认转化为double,
           * 所有最好hive中使用double替换float,否则存在double值精度丢失
           * spark还不支持decimal，所以hive中使用string来代替
           */
          case "float" => DoubleType
          case "tinyint" => IntegerType
          case "bigint" => LongType
          case typ if(typ.toLowerCase.startsWith("decimal")) => StringType
          //默认使用string类型
          case _ => StringType
        }, true)))
    LOG.warn(s"获取hive表schema:$tableSchema")
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
                    val col = cf(0)
                    cf(1) match {
                      case "ts" => TimeUtils.format(row.getAs(col).toString.toLong, TimeUtils.DATE)
                      case _ => TimeUtils.format(row.getAs(cf(0)).toString, cf(1), TimeUtils.DATE)
                    }
                }.filter(x => x != null)
               /*
                * 1.默认ods层hive表字段都为string类型，将数据类型全部转换为string类型，
                *   屏蔽mysql->spark->hive的多流程处理类型转换和spark-sql支持不足的问题
                * 2.如果有多个分区字段，默认使用最后一个字段分区值time(time.length-1)
                */
                Row((row.toSeq.map(x => x.toString) :+ time(time.length-1).toInt): _*)
            }
          }
          val dataFrame = hiveContext.createDataFrame(inputDataFrame.mapPartitions(partition), tableSchema)
          val writer = dataFrame.write.format("parquet").mode(SaveMode.Overwrite)
          writer.partitionBy("dt")
          writer.insertInto(s"${table}")
          LOG.warn(s"增量模式=>插入Hive表${table}数据量:[${dataFrame.count()}]]")
        }
      }
    }
    writeByMode()
  }
}
