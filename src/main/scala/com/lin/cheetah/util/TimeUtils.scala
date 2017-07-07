package com.lin.cheetah.util

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/**
  * Created by wenxuelin on 2017/7/6.
  */
object TimeUtils {
  val DATE = "yyyyMMdd"
  val DATE_HH = "yyyyMMddHH"
  val DATE_TIME = "yyyyMMddHHmmss"

  /**
    * 格式化时间，返回timestamp
    * @param time
    * @param format
    * @return
    */
  def timestamp(time: String, format: String): Long = {
    DateTime.parse(time, DateTimeFormat.forPattern(format)).getMillis
  }

  /**
    * 格式化时间，返回timestamp
    * @param time
    * @param format
    * @return
    */
  def format(time: String, format: String): String = {
    DateTime.parse(time, DateTimeFormat.forPattern(format)).toString(format)
  }

  /**
    * 格式化时间，返回格式化后的时间
    * @param time
    * @param fromFormat
    * @param toFormat
    * @return
    */
  def format(time: String, fromFormat: String, toFormat: String): String = {
    DateTime.parse(time, DateTimeFormat.forPattern(fromFormat)).toString(DateTimeFormat.forPattern(toFormat))
  }
}
