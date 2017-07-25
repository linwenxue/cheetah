package com.lin.cheetah.util

import scala.util.Try

/**
  * Created by wenxuelin on 2017/7/24.
  */
object ClassUtils {


  def getDefaultClassLoader: ClassLoader = getClass.getClassLoader

  /**
    * Get the Context ClassLoader on this thread or, if not present, the ClassLoader that
    * loaded Spark.
    *
    * This should be used whenever passing a ClassLoader to Class.ForName or finding the currently
    * active loader when setting up ClassLoader delegation chains.
    */
  def getContextOrDefaultClassLoader: ClassLoader =
  Option(Thread.currentThread().getContextClassLoader).getOrElse(getDefaultClassLoader)

  /** Determines whether the provided class is loadable in the current thread. */
  def classIsLoadable(clazz: String): Boolean = {
    // scalastyle:off classforname
    Try {
      Class.forName(clazz, false, getContextOrDefaultClassLoader)
    }.isSuccess
    // scalastyle:on classforname
  }

  // scalastyle:off classforname
  /** Preferred alternative to Class.forName(className) */
  def classForName(className: String): Class[_] = {
    Class.forName(className, true, getContextOrDefaultClassLoader)
    // scalastyle:on classforname
  }
}