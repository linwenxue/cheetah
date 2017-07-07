package com.lin.cheetah

import com.lin.cheetah.util.ClientArguments

/**
  * Created by wenxuelin on 2017/6/20.
  */
private[cheetah] object App {

  def main(args: Array[String]): Unit = {
    val clientArgument = new ClientArguments(args)
    val confPath = clientArgument.confPath
    val countTime = clientArgument.countTime
    val conf = new CheetahConf().init(confPath)
    conf.setConfString("countTime", countTime)
    val context = new CheetahContext(conf)
    context.setAppName("测试etl数据抽取")
      .setLogLevel("WARN")
      .execute()
  }
}
