package org.medicalsidefx.common.utils

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by sanjay on 12/23/14.
 */
object AersReacColumnExtractor {
  def main(args: Array[String]) {

    val sconf = new SparkConf().setMaster("local").setAppName("MedicalSideFx-AersDrugColumnExtractor")
    val sc = new SparkContext(sconf)

    val reachdrV1 = List("/data/aers/quarterly_files/aers_ascii_1999q1/ascii/reac/REAC99Q1.TXT","/data/aers/quarterly_files/aers_ascii_1999q2/ascii/reac/REAC99Q2.TXT","/data/aers/quarterly_files/aers_ascii_1999q3/ascii/reac/REAC99Q3.TXT","/data/aers/quarterly_files/aers_ascii_1999q4/ascii/reac/REAC99Q4.TXT","/data/aers/quarterly_files/aers_ascii_2000q1/ascii/reac/reac00q1.txt","/data/aers/quarterly_files/aers_ascii_2000q2/ascii/reac/reac00q2.txt","/data/aers/quarterly_files/aers_ascii_2000q3/ascii/reac/reac00q3.txt","/data/aers/quarterly_files/aers_ascii_2000q4/ascii/reac/reac00q4.txt","/data/aers/quarterly_files/aers_ascii_2001q1/ascii/reac/reac01q1.txt","/data/aers/quarterly_files/aers_ascii_2001q2/ascii/reac/reac01q2.txt","/data/aers/quarterly_files/aers_ascii_2001q3/ascii/reac/reac01q3.txt","/data/aers/quarterly_files/aers_ascii_2001q4/ascii/reac/reac01q4.txt","/data/aers/quarterly_files/aers_ascii_2002q1/ascii/reac/reac02q1.txt","/data/aers/quarterly_files/aers_ascii_2002q2/ascii/reac/reac02q2.txt","/data/aers/quarterly_files/aers_ascii_2002q3/ascii/reac/reac02q3.txt","/data/aers/quarterly_files/aers_ascii_2002q4/ascii/reac/reac02q4.txt","/data/aers/quarterly_files/aers_ascii_2003q1/ascii/reac/REAC03Q1.TXT","/data/aers/quarterly_files/aers_ascii_2003q2/ascii/reac/REAC03Q2.TXT","/data/aers/quarterly_files/aers_ascii_2003q3/ascii/reac/REAC03Q3.TXT","/data/aers/quarterly_files/aers_ascii_2003q4/ascii/reac/REAC03Q4.TXT","/data/aers/quarterly_files/AERS_ASCII_2004q1/ascii/reac/REAC04Q1.TXT","/data/aers/quarterly_files/AERS_ASCII_2004q2/ascii/reac/REAC04Q2.TXT","/data/aers/quarterly_files/AERS_ASCII_2004q3/ascii/reac/REAC04Q3.TXT","/data/aers/quarterly_files/AERS_ASCII_2004q4/ascii/reac/REAC04Q4.TXT","/data/aers/quarterly_files/AERS_ASCII_2005Q1/ascii/reac/REAC05Q1.TXT","/data/aers/quarterly_files/AERS_ASCII_2005Q2/ascii/reac/REAC05Q2.TXT","/data/aers/quarterly_files/AERS_ASCII_2005Q3/ascii/reac/REAC05Q3.TXT","/data/aers/quarterly_files/AERS_ASCII_2005Q4/ascii/reac/REAC05Q4.TXT","/data/aers/quarterly_files/AERS_ASCII_2006Q1/ascii/reac/REAC06Q1.TXT","/data/aers/quarterly_files/AERS_ASCII_2006Q2/ascii/reac/REAC06Q2.TXT","/data/aers/quarterly_files/AERS_ASCII_2006Q3/ascii/reac/REAC06Q3.TXT","/data/aers/quarterly_files/AERS_ASCII_2006Q4/ascii/reac/REAC06Q4.TXT","/data/aers/quarterly_files/AERS_ASCII_2007Q1/ascii/reac/REAC07Q1.TXT","/data/aers/quarterly_files/AERS_ASCII_2007Q2/ascii/reac/REAC07Q2.TXT","/data/aers/quarterly_files/AERS_ASCII_2007Q3/ascii/reac/REAC07Q3.TXT","/data/aers/quarterly_files/AERS_ASCII_2007Q4/ascii/reac/REAC07Q4.TXT","/data/aers/quarterly_files/AERS_ASCII_2008Q1/ascii/reac/REAC08Q1.TXT","/data/aers/quarterly_files/AERS_ASCII_2008Q2/ascii/reac/REAC08Q2.TXT","/data/aers/quarterly_files/aers_ascii_2008q3/ascii/reac/REAC08Q3.TXT","/data/aers/quarterly_files/aers_ascii_2008q4/ascii/reac/REAC08Q4.TXT","/data/aers/quarterly_files/aers_ascii_2009q1/ascii/reac/REAC09Q1.TXT","/data/aers/quarterly_files/aers_ascii_2009q2/ascii/reac/REAC09Q2.TXT","/data/aers/quarterly_files/aers_ascii_2009q3/ascii/reac/REAC09Q3.TXT","/data/aers/quarterly_files/aers_ascii_2009q4/ascii/reac/REAC09Q4.TXT","/data/aers/quarterly_files/aers_ascii_2010q1/ascii/reac/REAC10Q1.TXT","/data/aers/quarterly_files/aers_ascii_2010q2/ascii/reac/REAC10Q2.TXT","/data/aers/quarterly_files/aers_ascii_2010q3/ascii/reac/REAC10Q3.TXT","/data/aers/quarterly_files/aers_ascii_2010q4/ascii/reac/REAC10Q4.TXT","/data/aers/quarterly_files/aers_ascii_2011q1/ascii/reac/REAC11Q1.TXT","/data/aers/quarterly_files/aers_ascii_2011q2/ascii/reac/REAC11Q2.TXT","/data/aers/quarterly_files/aers_ascii_2011q3/ascii/reac/REAC11Q3.TXT","/data/aers/quarterly_files/aers_ascii_2011q4/ascii/reac/REAC11Q4.TXT","/data/aers/quarterly_files/aers_ascii_2012q1/ascii/reac/REAC12Q1.TXT","/data/aers/quarterly_files/aers_ascii_2012q2/ascii/reac/REAC12Q2.TXT","/data/aers/quarterly_files/aers_ascii_2012q3/ascii/reac/REAC12Q3.TXT")
    val reachdrV2 = List("/data/aers/quarterly_files/faers_ascii_2012q4/ascii/reac/reac12q4.txt","/data/aers/quarterly_files/faers_ascii_2013q1/ascii/reac/REAC13Q1.txt","/data/aers/quarterly_files/faers_ascii_2013q2/ascii/reac/REAC13Q2.txt","/data/aers/quarterly_files/AERS_ASCII_2013q3/ascii/reac/REAC13Q3.txt","/data/aers/quarterly_files/FAERS_ASCII_2013Q4/ascii/reac/REAC13Q4.txt","/data/aers/quarterly_files/faers_ascii_2014q1/ascii/reac/REAC14Q1.txt")

    val reacList = Map("reachdrV1" -> reachdrV1,
      "reachdrV2" -> reachdrV2)

    var outFile = ""

    for ((key,value) <- reacList) {
      for (a <- value) {
        var reacRdd = sc.textFile(a)
        try {
          outFile = a.reverse.split('/')(0).split('.')(1).reverse
        }
        catch {
          case e: Exception => println(a.toString + " - Exception");
        }
        if (outFile != null) {
          try {
            var reacRddMap = reacRdd.map(line => {
              var tokens = line.split('$')
              if (tokens.length >= 2 && !line.contains("ISR$PT") && !line.contains("primaryid$caseid$pt")) {
                if (key.toString().contains("reachdrV1")) {
                  tokens(0) + "\t" + tokens(1)
                }
                else if (key.toString().contains("reachdrV2")) {
                  tokens(0) + "\t" + tokens(2)
                }
                else {
                  ""
                }
              }
              else {
                ""
              }
            })
            reacRddMap.saveAsTextFile("/data/aers/msfx/reac/" + outFile)
            println("SUCCESSFUL " + a.toString)
          }
          catch {
            case e: Exception => println("FAILED " + a.toString)
          }
        }
      }
    }
  }
}
