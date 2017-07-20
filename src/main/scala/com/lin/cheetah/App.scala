package com.lin.cheetah

import com.lin.cheetah.util.ClientArguments
import org.slf4j.LoggerFactory

/**
  * Created by wenxuelin on 2017/6/20.
  */
private[cheetah] object App {
  val LOG = LoggerFactory.getLogger("Cheetah App")

  def main(args: Array[String]): Unit = {
    val startTime = System.nanoTime()
    val clientArgument = new ClientArguments(args)
    val confPath = clientArgument.confPath
    val countTime = clientArgument.countTime
    val conf = new CheetahConf().init(confPath)
    conf.setConfString("countTime", countTime)

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run() = {
        //添加异常处理逻辑，如资源释放等
        LOG.warn("程序结束")
      }
    })

    val context = new CheetahContext(conf)
      .setAppName("测试etl数据抽取")
      .setLogLevel("WARN")
    context.execute()
    LOG.warn("程序运行时间："+(System.nanoTime()-startTime)/1000000000+"s")
  }
}
