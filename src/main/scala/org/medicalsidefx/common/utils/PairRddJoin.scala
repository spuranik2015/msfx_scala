package org.medicalsidefx.common.utils

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

/**
 * Created by sansub01 on 11/19/14.
 */
object PairRddJoin {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: PairRddJoin <file1>   <file2>")
      System.exit(786)
    }
/*    rdd1.txt

    1~4,5,6,7
    2~4,5
    3~6,7

    rdd2.txt

    4~1001,1000,1002,1003
    5~1004,1001,1006,1007
    6~1007,1009,1005,1008
    7~1011,1012,1013,1010

*/
    val sconf = new SparkConf().setMaster("local").setAppName("MedicalSideFx-PairRddJoin")
    val sc = new SparkContext(sconf)

//    val rdd1 = args(0)
//    val rdd2 = args(1)

    val rdd1 = "/Users/sanjay/mycode/data/scala_examples/pairrddjoin/rdd1.txt"
    val rdd2 = "/Users/sanjay/mycode/data/scala_examples/pairrddjoin/rdd2.txt"

    val rdd1InvIndex = sc.textFile(rdd1).map(x => (x.split('~')(0), x.split('~')(1))).flatMapValues(str => str.split(',')).map(str => (str._2, str._1))
    val rdd2Pair = sc.textFile(rdd2).map(str => (str.split('~')(0), str.split('~')(1)))
    rdd1InvIndex.join(rdd2Pair).map(str => str._2).groupByKey().collect().foreach(println)
  }
}
