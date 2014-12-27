package org.medicalsidefx.common.utils

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by sanjay on 12/23/14.
 */
object AersDemoColumnExtractor {
  def main(args: Array[String]) {
    val sconf = new SparkConf().setMaster("local").setAppName("MedicalSideFx-AersDemoColumnExtractor")
    val sc = new SparkContext(sconf)

    val demohdrV1 = List("/data/aers/quarterly_files/aers_ascii_1999q1/ascii/demo/DEMO99Q1.TXT", "/data/aers/quarterly_files/aers_ascii_1999q2/ascii/demo/DEMO99Q2.TXT", "/data/aers/quarterly_files/aers_ascii_1999q3/ascii/demo/DEMO99Q3.TXT", "/data/aers/quarterly_files/aers_ascii_1999q4/ascii/demo/DEMO99Q4.TXT", "/data/aers/quarterly_files/aers_ascii_2000q1/ascii/demo/demo00q1.txt", "/data/aers/quarterly_files/aers_ascii_2000q2/ascii/demo/demo00q2.txt", "/data/aers/quarterly_files/aers_ascii_2000q3/ascii/demo/demo00q3.txt", "/data/aers/quarterly_files/aers_ascii_2000q4/ascii/demo/demo00q4.txt", "/data/aers/quarterly_files/aers_ascii_2001q1/ascii/demo/demo01q1.txt", "/data/aers/quarterly_files/aers_ascii_2001q2/ascii/demo/demo01q2.txt", "/data/aers/quarterly_files/aers_ascii_2001q3/ascii/demo/demo01q3.txt", "/data/aers/quarterly_files/aers_ascii_2001q4/ascii/demo/demo01q4.txt", "/data/aers/quarterly_files/aers_ascii_2002q1/ascii/demo/demo02q1.txt")
    val demohdrV2 = List("/data/aers/quarterly_files/aers_ascii_2002q2/ascii/demo/demo02q2.txt")
    val demohdrV3 = List("/data/aers/quarterly_files/aers_ascii_2002q3/ascii/demo/demo02q3.txt", "/data/aers/quarterly_files/aers_ascii_2002q4/ascii/demo/demo02q4.txt", "/data/aers/quarterly_files/aers_ascii_2003q1/ascii/demo/DEMO03Q1.TXT", "/data/aers/quarterly_files/aers_ascii_2003q2/ascii/demo/DEMO03Q2.TXT", "/data/aers/quarterly_files/aers_ascii_2003q3/ascii/demo/DEMO03Q3.TXT", "/data/aers/quarterly_files/aers_ascii_2003q4/ascii/demo/DEMO03Q4.TXT", "/data/aers/quarterly_files/AERS_ASCII_2004q1/ascii/demo/DEMO04Q1.TXT", "/data/aers/quarterly_files/AERS_ASCII_2004q2/ascii/demo/DEMO04Q2.TXT", "/data/aers/quarterly_files/AERS_ASCII_2004q3/ascii/demo/DEMO04Q3.TXT", "/data/aers/quarterly_files/AERS_ASCII_2004q4/ascii/demo/DEMO04Q4.TXT", "/data/aers/quarterly_files/AERS_ASCII_2005Q1/ascii/demo/DEMO05Q1.TXT", "/data/aers/quarterly_files/AERS_ASCII_2005Q2/ascii/demo/DEMO05Q2.TXT")
    val demohdrV4 = List("/data/aers/quarterly_files/AERS_ASCII_2005Q3/ascii/demo/DEMO05Q3.TXT", "/data/aers/quarterly_files/AERS_ASCII_2005Q4/ascii/demo/DEMO05Q4.TXT", "/data/aers/quarterly_files/AERS_ASCII_2006Q1/ascii/demo/DEMO06Q1.TXT", "/data/aers/quarterly_files/AERS_ASCII_2006Q2/ascii/demo/DEMO06Q2.TXT", "/data/aers/quarterly_files/AERS_ASCII_2006Q3/ascii/demo/DEMO06Q3.TXT", "/data/aers/quarterly_files/AERS_ASCII_2006Q4/ascii/demo/DEMO06Q4.TXT", "/data/aers/quarterly_files/AERS_ASCII_2007Q1/ascii/demo/DEMO07Q1.TXT", "/data/aers/quarterly_files/AERS_ASCII_2007Q2/ascii/demo/DEMO07Q2.TXT", "/data/aers/quarterly_files/AERS_ASCII_2007Q3/ascii/demo/DEMO07Q3.TXT", "/data/aers/quarterly_files/AERS_ASCII_2007Q4/ascii/demo/DEMO07Q4.TXT", "/data/aers/quarterly_files/AERS_ASCII_2008Q1/ascii/demo/DEMO08Q1.TXT", "/data/aers/quarterly_files/AERS_ASCII_2008Q2/ascii/demo/DEMO08Q2.TXT", "/data/aers/quarterly_files/aers_ascii_2008q3/ascii/demo/DEMO08Q3.TXT", "/data/aers/quarterly_files/aers_ascii_2008q4/ascii/demo/DEMO08Q4.TXT", "/data/aers/quarterly_files/aers_ascii_2009q1/ascii/demo/DEMO09Q1.TXT", "/data/aers/quarterly_files/aers_ascii_2009q2/ascii/demo/DEMO09Q2.TXT", "/data/aers/quarterly_files/aers_ascii_2009q3/ascii/demo/DEMO09Q3.TXT", "/data/aers/quarterly_files/aers_ascii_2009q4/ascii/demo/DEMO09Q4.TXT", "/data/aers/quarterly_files/aers_ascii_2010q1/ascii/demo/DEMO10Q1.TXT", "/data/aers/quarterly_files/aers_ascii_2010q2/ascii/demo/DEMO10Q2.TXT", "/data/aers/quarterly_files/aers_ascii_2010q3/ascii/demo/DEMO10Q3.TXT", "/data/aers/quarterly_files/aers_ascii_2010q4/ascii/demo/DEMO10Q4.TXT", "/data/aers/quarterly_files/aers_ascii_2011q1/ascii/demo/DEMO11Q1.TXT", "/data/aers/quarterly_files/aers_ascii_2011q2/ascii/demo/DEMO11Q2.TXT", "/data/aers/quarterly_files/aers_ascii_2011q3/ascii/demo/DEMO11Q3.TXT", "/data/aers/quarterly_files/aers_ascii_2011q4/ascii/demo/DEMO11Q4.TXT", "/data/aers/quarterly_files/aers_ascii_2012q1/ascii/demo/DEMO12Q1.TXT", "/data/aers/quarterly_files/aers_ascii_2012q2/ascii/demo/DEMO12Q2.TXT", "/data/aers/quarterly_files/aers_ascii_2012q3/ascii/demo/DEMO12Q3.TXT")
    val demohdrV5 = List("/data/aers/quarterly_files/AERS_ASCII_2013q3/ascii/demo/DEMO13Q3.txt", "/data/aers/quarterly_files/faers_ascii_2012q4/ascii/demo/demo12q4.txt", "/data/aers/quarterly_files/faers_ascii_2013q1/ascii/demo/DEMO13Q1.txt", "/data/aers/quarterly_files/faers_ascii_2013q2/ascii/demo/DEMO13Q2.txt", "/data/aers/quarterly_files/FAERS_ASCII_2013Q4/ascii/demo/DEMO13Q4.txt", "/data/aers/quarterly_files/faers_ascii_2014q1/ascii/demo/DEMO14Q1.txt")

//    val demoList = List(demohdrV1, demohdrV2, demohdrV3, demohdrV4, demohdrV5)
    val demoList = Map("demohdrV1" -> demohdrV1,
                        "demohdrV2" -> demohdrV2,
                        "demohdrV3" -> demohdrV3,
                        "demohdrV4" -> demohdrV4,
                        "demohdrV5" -> demohdrV5)

    var outFile = ""
      for ((key,value) <- demoList) {
          for (a <- value) {
            var demoRdd = sc.textFile(a)
            try {
              outFile = a.reverse.split('/')(0).split('.')(1).reverse
            }
            catch {
              case e: Exception => println(a.toString + " - Exception");
            }
            if (outFile != null) {
              try {
                var demoRddMap = demoRdd.map(line => {
                  var tokens = line.split('$')
                  if (tokens.length >= 13 && !line.contains("ISR$CASE$I_F_COD$FOLL_SEQ") && !line.contains("primaryid$caseid$caseversion")) {
                    if (key.toString().contains("demohdrV5")) {
                      tokens(0) + "\t" + tokens(4) + "\t" + tokens(11) + "\t" + tokens(12)
                    }
                    else {
                      tokens(0) + "\t" + tokens(5) + "\t" + tokens(11) + "\t" + tokens(12)
                    }
                  }
                  else {
                    ""
                  }
                })
                demoRddMap.saveAsTextFile("/data/aers/msfx/demo/" + outFile)
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
