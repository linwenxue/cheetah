package com.lin.cheetah.execution

import org.apache.spark.sql.DataFrame

/**
  * Created by wenxuelin on 2017/6/23.
  */
trait Writer extends Serializable{
  def write(dataFrame: DataFrame): Unit
}
