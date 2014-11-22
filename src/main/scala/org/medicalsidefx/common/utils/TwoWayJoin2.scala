package org.medicalsidefx.common.utils

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

import scala.collection.mutable.ArrayBuffer

/**
 * Created by sansub01 on 11/19/14.
 */
object TwoWayJoin2 {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: TwoWayJoinCount <file1>   <file2>")
      System.exit(12)
    }

    val sconf = new SparkConf().setMaster("local").setAppName("MedicalSideFx-TwoWayJoin")

    val sc = new SparkContext(sconf)

    val file1 = args(0)
    val file2 = args(1)

    val file1Rdd = sc.textFile(file1).map(x => (x.split(",")(0), x.split(",")(1)))
    val file2Rdd = sc.textFile(file2).map(x => (x.split(",")(0), x.split(",")(1))).reduceByKey((v1,v2) => v1+"|"+v2)

    file1Rdd.collect().foreach(println)
    file2Rdd.collect().foreach(println)

    file1Rdd.join(file2Rdd).collect().foreach( e => println(e.toString.replace("(","").replace(")","")))


//    file1Rdd.join(file2Rdd).saveAsTextFile("C:\\sanjay\\kachra\\output\\twowayjoin2")
//    val file2RddGrp = file2Rdd.groupByKey()
//    val f1f2grp = file1Rdd.join(file2RddGrp.mapValues(names => names.toSet))
//    f1f2grp.collect().foreach(println)

  }
}
