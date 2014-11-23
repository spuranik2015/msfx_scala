package org.medicalsidefx.common.utils

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object WordCount {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: WordCount <file>")
      System.exit(1)
    }

    val sconf = new SparkConf().setMaster("local").setAppName("MedicalSideFx-ScalaAnalytics")
    val sc = new SparkContext(sconf)
    val inputDir = args(0)
    val outDir = args(1)
    /*
    val counts = sc.textFile(file).
      flatMap(line => line.split("\\W")).
      map(word => (word,1)).
      reduceByKey((v1,v2) => v1+v2).foreach(println)
      */

    sc.textFile(inputDir).
      flatMap(line => line.split("\\W")).
      map(word => (word,1)).
      reduceByKey((v1,v2) => v1+v2).saveAsTextFile(outDir+"/out_"+System.currentTimeMillis)
//    counts.take(10).foreach(println)
  }
}
