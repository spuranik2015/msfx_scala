package org.medicalsidefx.common.utils

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

/**
 * Created by sansub01 on 11/20/14.
 */
object PageCounts2 {
  def main(args: Array[String]) {

    val sconf = new SparkConf().setMaster("local").setAppName("MedicalSideFx-PageCounts")

    val sc = new SparkContext(sconf)

    val pagecounts = sc.textFile("/Users/sansub01/mycode/knowledge/spark_ampcamp_2014/data/pagecounts")

    pagecounts.map(line => (line.split(" ")(1),1)).reduceByKey((v1,v2) => v1+v2).groupByKey().collect().foreach(println)
  }

}