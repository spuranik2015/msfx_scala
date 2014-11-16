package org.medicalsidefx.common.utils

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object HelloWorld {
    def main(args: Array[String]) {
      println("Hello, Cruel world!")
      val conf = new SparkConf()
      			     .setMaster("local2")
      			     .setAppName("foofla")
      			     .set("spark.executor.memory","1g")
      			     .set("spark.rdd.compress", "true")
      			     .set("spark.storage.memoryFraction", "1")
      val sc = new SparkContext(conf)
      val data = sc.parallelize(1 to 1000000).collect().filter(_<1000)
      data.foreach(println)
    }
  }