package org.medicalsidefx.common.utils

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by sanjay on 12/23/14.
 */
object AersDrugColumnExtractor {

  val sconf = new SparkConf().setMaster("local").setAppName("MedicalSideFx-AersDrugColumnExtractor")
  val sc = new SparkContext(sconf)

  val drughdrV1 =  List("\"/data/aers/quarterly_files/aers_ascii_1999q1/ascii/drug/DRUG99Q1.TXT\",\"/data/aers/quarterly_files/aers_ascii_1999q2/ascii/drug/DRUG99Q2.TXT\",\"/data/aers/quarterly_files/aers_ascii_1999q3/ascii/drug/DRUG99Q3.TXT\",\"/data/aers/quarterly_files/aers_ascii_1999q4/ascii/drug/DRUG99Q4.TXT\",\"/data/aers/quarterly_files/aers_ascii_2000q1/ascii/drug/drug00q1.txt\",\"/data/aers/quarterly_files/aers_ascii_2000q2/ascii/drug/drug00q2.txt\",\"/data/aers/quarterly_files/aers_ascii_2000q3/ascii/drug/drug00q3.txt\",\"/data/aers/quarterly_files/aers_ascii_2000q4/ascii/drug/drug00q4.txt\",\"/data/aers/quarterly_files/aers_ascii_2001q1/ascii/drug/drug01q1.txt\",\"/data/aers/quarterly_files/aers_ascii_2001q2/ascii/drug/drug01q2.txt\",\"/data/aers/quarterly_files/aers_ascii_2001q3/ascii/drug/drug01q3.txt\",\"/data/aers/quarterly_files/aers_ascii_2001q4/ascii/drug/drug01q4.txt\",\"/data/aers/quarterly_files/aers_ascii_2002q1/ascii/drug/drug02q1.txt\"")
  val drughdrV2 =  List("")
  val drughdrV3 = List("")
  val drughdrV4 = List("")

  val demoList = List(drughdrV1,drughdrV2,drughdrV3,drughdrV4)

  var outFile = ""

  for (b <- demoList) {
    for (a <- b) {
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
            if (tokens.length >= 13 && !line.contains("ISR$CASE$I_F_COD$FOLL_SEQ") && !line.contains("primaryid$caseid$caseversion")){
              tokens(0) + "\t" + tokens(5) + "\t" + tokens(11) + "\t" + tokens(12)
            }
            else {
                 ""
            }
          })
          demoRddMap.saveAsTextFile("/data/aers/msfx/demo/" + outFile)
          println("SUCCESSFUL "+a.toString)
        }
        catch {
          case e: Exception => println("FAILED "+a.toString)
        }
      }
    }
  }
}
