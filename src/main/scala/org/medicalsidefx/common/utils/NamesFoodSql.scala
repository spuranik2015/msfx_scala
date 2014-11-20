package org.medicalsidefx.common.utils

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by sansub01 on 11/19/14.
 */
object NamesFoodSql {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: NamesFoodSql <file1>")
      System.exit(123)
    }
    val file1 = args(0)

    val sconf = new SparkConf().setMaster("local").setAppName("MedicalSideFx-NamesFoodSql")
    val sc = new SparkContext(sconf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val peoplefood = sc.textFile(file1)

    val schemaString = "name,food"

    import org.apache.spark.sql._

    val schema =
      StructType(
        schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))

    val rowRDD = peoplefood.map(_.split(",")).map(p => Row(p(0), p(1).trim))

    val peopleSchemaRDD = sqlContext.applySchema(rowRDD, schema)

    peopleSchemaRDD.registerTempTable("peoplefood")

    val results = sqlContext.sql("SELECT name,food FROM peoplefood where food = 'pizza' OR  food = 'dosa'")

    results.map(t => "Name:" + t(0) + " , Food:"+t(1)).collect().foreach(println)  }
}
