package org.medicalsidefx.common.utils

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by sansub01 on 11/19/14.
 */
object ParquetSql {
  def main(args: Array[String]) {

    val sconf = new SparkConf().setMaster("local").setAppName("MedicalSideFx-NamesFoodSql")
    val sc = new SparkContext(sconf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    sqlContext.setConf("spark.sql.parquet.binaryAsString", "true")
    sqlContext.getAllConfs.foreach(println)
    val wikiData = sqlContext.parquetFile("/Users/sansub01/mycode/knowledge/spark_ampcamp_2014/data/wiki_parquet")
    wikiData.registerTempTable("wikiData")
    val results = sqlContext.sql("SELECT username, COUNT(*) AS cnt FROM wikiData WHERE username <> '' GROUP BY username ORDER BY cnt DESC LIMIT 10")
    results.collect().foreach(println)
  }

}
