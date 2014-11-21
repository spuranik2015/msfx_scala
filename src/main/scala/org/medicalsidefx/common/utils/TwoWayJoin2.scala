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
    val file2Rdd = sc.textFile(file2).map(x => (x.split(",")(0), x.split(",")(1)))
    val file2RddGrp = file2Rdd.groupByKey()
    file1Rdd.join(file2RddGrp.mapValues(names => names.toSet)).collect().foreach(println)


    //    val file1Rdd = sc.textFile(file1).map(x => (x.split(",")(0), x.split(",")(1)))
//    val file2Rdd = sc.textFile(file2).map(x => (x.split(",")(0), x.split(",")(1))).groupByKey()
    //    file1Rdd.leftOuterJoin(file2Rdd).groupByKey().map{ case (k,v) => (k.toArray,v.toArray)}.collect().foreach(println)
//    file1Rdd.join(file2Rdd).foreach(println)
    /*
    val file2RddGrp = file2Rdd.groupByKey().map
    { case (k,v) => (k,v.toArray) } // map(x:ArrayBuffer[String] => x.toArray())
  */

//    val file2RddGrp = file2Rdd.groupByKey()
//    val f1Joinf2 = file1Rdd.join(file2RddGrp)

//    file1Rdd.join(file2RddGrp).collect().foreach(println)
    //file1Rdd.join(file2RddGrp.mapValues(names => names.toSet)).collect().foreach(println)
//    file1Rdd.join(file2Rdd.groupByKey().mapValues(names => names.toSet)).collect().foreach(println)
/*
//    file2Rdd.groupByKey().join(file1Rdd.groupByKey()).foreach(line => println(line))
//    file1Rdd.groupByKey().join(file2Rdd.groupByKey()).foreach(line => println(line))
*/
  }
}
