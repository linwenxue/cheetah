package com.lin.cheetah.execution.reader

import org.apache.spark.sql.DataFrame

/**
  * Created by wenxuelin on 2017/6/23.
  */
trait Reader extends Serializable{
  def read(): DataFrame
}
