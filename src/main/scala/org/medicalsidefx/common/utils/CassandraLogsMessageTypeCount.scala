package org.medicalsidefx.common.utils

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

object CassandraLogsMessageTypeCount {
  def main(args: Array[String]) {
    /*
    if (args.length < 1) {
      System.err.println("Usage: WordCount <file>")
      System.exit(1)
    }
*/
    val sconf = new SparkConf().setMaster("local").setAppName("MedicalSideFx-CassandraLogsMessageTypeCount")
    val sc = new SparkContext(sconf)
//    val inputDir = args(0)
//    val outDir = args(1)

    val inputDir = "/Users/sanjay/mycode/data/scala_examples/pairrddjoin/cassandralogs.txt"
    val outDir = "/Users/sanjay/mycode/data/scala_examples/pairrddjoin/"
    var outFile = outDir+"/"+inputDir.reverse.split('/')(0).split('.')(1).reverse + "_" + System.currentTimeMillis

    sc.textFile(inputDir).map(line => line.replace("\"", "")).map(line => (line.split(' ')(0) + " " + line.split(' ')(2), 1)).reduceByKey((v1,v2) => v1+v2).collect().foreach(println)
//    counts.take(10).foreach(println)

  }
}
