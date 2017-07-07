package com.lin.cheetah.util

import org.apache.commons.cli.{BasicParser, Options}

/**
  * Created by wenxuelin on 2017/6/26.
  */
private[cheetah] class ClientArguments(val args: Array[String]) {
  private val parser = new BasicParser()
  private val options = new Options()
  options.addOption("c", "conf", true, "配置文件")
  options.addOption("t", "count_time", true, "统计时间")
  private val commandLine = parser.parse(options, args)

  val confPath = commandLine.hasOption("c") match {
    case true => commandLine.getOptionValue("c")
    case false => throw new IllegalArgumentException("配置文件路径未指定，-c 或者 --conf 参数指定")
  }

  val countTime = commandLine.hasOption("t") match {
    case true => {
      try {
        commandLine.getOptionValue("t").split(",").map {
          time =>
            TimeUtils.format(time, "yyyyMMddHH")
        }.mkString(",")
      } catch {
        case e: Exception => throw new RuntimeException(s"错误信息：${e.getMessage}")
      }
    }
    case false => throw new RuntimeException("-t参数为空")
  }
}
