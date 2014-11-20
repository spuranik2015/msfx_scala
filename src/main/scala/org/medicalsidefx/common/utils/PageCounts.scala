package org.medicalsidefx.common.utils

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

/**
 * Created by sansub01 on 11/20/14.
 */
object PageCounts {
  def main(args: Array[String]) {

    val sconf = new SparkConf().setMaster("local").setAppName("MedicalSideFx-PageCounts")
    val sc = new SparkContext(sconf)

    val pagecounts = sc.textFile("/Users/sansub01/mycode/knowledge/spark_ampcamp_2014/data/pagecounts")
    pagecounts.take(10)
    pagecounts.take(10).foreach(println)
    pagecounts.count
    val enPages = pagecounts.filter(_.split(" ")(1) == "en").cache
    enPages.count
    val enTuples = enPages.map(line => line.split(" "))
    val enKeyValuePairs = enTuples.map(line => (line(0).substring(0, 8), line(3).toInt))
    enKeyValuePairs.reduceByKey(_+_, 1).collect
    enPages.map(line => line.split(" ")).map(line => (line(0).substring(0, 8), line(3).toInt)).reduceByKey(_+_, 1).collect
    enPages.map(l => l.split(" ")).map(l => (l(2), l(3).toInt)).reduceByKey(_+_, 40).filter(x => x._2 > 200000).map(x => (x._2, x._1)).collect.foreach(println)
  }

}
