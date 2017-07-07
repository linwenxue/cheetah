package com.lin.cheetah

import com.lin.cheetah.execution.{HiveWriter, MysqlReader, Reader, Writer}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by wenxuelin on 2017/6/23.
  */
private[cheetah] class CheetahContext(conf: CheetahConf) extends Serializable{
  private lazy val sparkConf: SparkConf = new SparkConf()
  private lazy val sc: SparkContext = new SparkContext(sparkConf)
  private lazy val sqlContext: SQLContext = new SQLContext(sc)
  protected var reader: Reader = new MysqlReader(sqlContext, conf)
  protected var writer: Writer = new HiveWriter(sc, conf)

  private[cheetah] def get(key: String): String = conf.get(key)

  private[cheetah] def getConf(): CheetahConf = {
    conf
  }

  def setAppName(name: String): CheetahContext = {
    sparkConf.setAppName(name)
    this
  }

  def read(): DataFrame = {
    reader.read
  }

  def write(dataFrame: DataFrame): Unit = {
    writer.write(dataFrame)
  }

  def execute(): Unit = {
    writer.write(reader.read)
  }

  def getSparkConf: SparkConf = sparkConf

  def getSparkContext: SparkContext = sc

  def getSparkSqlContext: SQLContext = sqlContext

  def setReader(reader: Reader): Unit = {
    this.reader = reader
  }

  def setWriter(writer: Writer): Unit = {
    this.writer = writer
  }

  def getReader(): Reader = reader

  def getWriter(): Writer = writer

  def setLogLevel(level: String): CheetahContext ={
    sc.setLogLevel(level)
    this
  }
}
