package org.medicalsidefx.common.utils

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by sanjay on 12/23/14.
 */
object AersDrugColumnExtractor {
  def main(args: Array[String]) {

    val sconf = new SparkConf().setMaster("local").setAppName("MedicalSideFx-AersDrugColumnExtractor")
    val sc = new SparkContext(sconf)

    val drughdrV1 = List("/data/aers/quarterly_files/aers_ascii_1999q1/ascii/drug/DRUG99Q1.TXT","/data/aers/quarterly_files/aers_ascii_1999q2/ascii/drug/DRUG99Q2.TXT","/data/aers/quarterly_files/aers_ascii_1999q3/ascii/drug/DRUG99Q3.TXT","/data/aers/quarterly_files/aers_ascii_1999q4/ascii/drug/DRUG99Q4.TXT","/data/aers/quarterly_files/aers_ascii_2000q1/ascii/drug/drug00q1.txt","/data/aers/quarterly_files/aers_ascii_2000q2/ascii/drug/drug00q2.txt","/data/aers/quarterly_files/aers_ascii_2000q3/ascii/drug/drug00q3.txt","/data/aers/quarterly_files/aers_ascii_2000q4/ascii/drug/drug00q4.txt","/data/aers/quarterly_files/aers_ascii_2001q1/ascii/drug/drug01q1.txt","/data/aers/quarterly_files/aers_ascii_2001q2/ascii/drug/drug01q2.txt","/data/aers/quarterly_files/aers_ascii_2001q3/ascii/drug/drug01q3.txt","/data/aers/quarterly_files/aers_ascii_2001q4/ascii/drug/drug01q4.txt","/data/aers/quarterly_files/aers_ascii_2002q1/ascii/drug/drug02q1.txt")
    val drughdrV2 = List("/data/aers/quarterly_files/aers_ascii_2002q2/ascii/drug/drug02q2.txt","/data/aers/quarterly_files/aers_ascii_2002q3/ascii/drug/drug02q3.txt","/data/aers/quarterly_files/aers_ascii_2002q4/ascii/drug/drug02q4.txt","/data/aers/quarterly_files/aers_ascii_2003q1/ascii/drug/DRUG03Q1.TXT","/data/aers/quarterly_files/aers_ascii_2003q2/ascii/drug/DRUG03Q2.TXT","/data/aers/quarterly_files/aers_ascii_2003q3/ascii/drug/DRUG03Q3.TXT","/data/aers/quarterly_files/aers_ascii_2003q4/ascii/drug/DRUG03Q4.TXT","/data/aers/quarterly_files/AERS_ASCII_2004q1/ascii/drug/DRUG04Q1.TXT","/data/aers/quarterly_files/AERS_ASCII_2004q2/ascii/drug/DRUG04Q2.TXT","/data/aers/quarterly_files/AERS_ASCII_2004q3/ascii/drug/DRUG04Q3.TXT","/data/aers/quarterly_files/AERS_ASCII_2004q4/ascii/drug/DRUG04Q4.TXT","/data/aers/quarterly_files/AERS_ASCII_2005Q1/ascii/drug/DRUG05Q1.TXT","/data/aers/quarterly_files/AERS_ASCII_2005Q2/ascii/drug/DRUG05Q2.TXT","/data/aers/quarterly_files/AERS_ASCII_2005Q3/ascii/drug/DRUG05Q3.TXT","/data/aers/quarterly_files/AERS_ASCII_2005Q4/ascii/drug/DRUG05Q4.TXT","/data/aers/quarterly_files/AERS_ASCII_2006Q1/ascii/drug/DRUG06Q1.TXT","/data/aers/quarterly_files/AERS_ASCII_2006Q2/ascii/drug/DRUG06Q2.TXT","/data/aers/quarterly_files/AERS_ASCII_2006Q3/ascii/drug/DRUG06Q3.TXT","/data/aers/quarterly_files/AERS_ASCII_2006Q4/ascii/drug/DRUG06Q4.TXT","/data/aers/quarterly_files/AERS_ASCII_2007Q1/ascii/drug/DRUG07Q1.TXT","/data/aers/quarterly_files/AERS_ASCII_2007Q2/ascii/drug/DRUG07Q2.TXT","/data/aers/quarterly_files/AERS_ASCII_2007Q3/ascii/drug/DRUG07Q3.TXT","/data/aers/quarterly_files/AERS_ASCII_2007Q4/ascii/drug/DRUG07Q4.TXT","/data/aers/quarterly_files/AERS_ASCII_2008Q1/ascii/drug/DRUG08Q1.TXT","/data/aers/quarterly_files/AERS_ASCII_2008Q2/ascii/drug/DRUG08Q2.TXT","/data/aers/quarterly_files/aers_ascii_2008q3/ascii/drug/DRUG08Q3.TXT","/data/aers/quarterly_files/aers_ascii_2008q4/ascii/drug/DRUG08Q4.TXT","/data/aers/quarterly_files/aers_ascii_2009q1/ascii/drug/DRUG09Q1.TXT","/data/aers/quarterly_files/aers_ascii_2009q2/ascii/drug/DRUG09Q2.TXT","/data/aers/quarterly_files/aers_ascii_2009q3/ascii/drug/DRUG09Q3.TXT","/data/aers/quarterly_files/aers_ascii_2009q4/ascii/drug/DRUG09Q4.TXT","/data/aers/quarterly_files/aers_ascii_2010q1/ascii/drug/DRUG10Q1.TXT","/data/aers/quarterly_files/aers_ascii_2010q2/ascii/drug/DRUG10Q2.TXT","/data/aers/quarterly_files/aers_ascii_2010q3/ascii/drug/DRUG10Q3.TXT","/data/aers/quarterly_files/aers_ascii_2010q4/ascii/drug/DRUG10Q4.TXT","/data/aers/quarterly_files/aers_ascii_2011q1/ascii/drug/DRUG11Q1.TXT","/data/aers/quarterly_files/aers_ascii_2011q2/ascii/drug/DRUG11Q2.TXT","/data/aers/quarterly_files/aers_ascii_2011q3/ascii/drug/DRUG11Q3.TXT","/data/aers/quarterly_files/aers_ascii_2011q4/ascii/drug/DRUG11Q4.TXT","/data/aers/quarterly_files/aers_ascii_2012q1/ascii/drug/DRUG12Q1.TXT","/data/aers/quarterly_files/aers_ascii_2012q2/ascii/drug/DRUG12Q2.TXT","/data/aers/quarterly_files/aers_ascii_2012q3/ascii/drug/DRUG12Q3.TXT")
    val drughdrV3 = List("/data/aers/quarterly_files/faers_ascii_2012q4/ascii/drug/drug12q4.txt","/data/aers/quarterly_files/faers_ascii_2013q1/ascii/drug/DRUG13Q1.txt","/data/aers/quarterly_files/faers_ascii_2013q2/ascii/drug/DRUG13Q2.txt")
    val drughdrV4 = List("/data/aers/quarterly_files/AERS_ASCII_2013q3/ascii/drug/DRUG13Q3.txt","/data/aers/quarterly_files/FAERS_ASCII_2013Q4/ascii/drug/DRUG13Q4.txt","/data/aers/quarterly_files/faers_ascii_2014q1/ascii/drug/DRUG14Q1.txt")

    val drugList = Map("drughdrV1" -> drughdrV1,
      "drughdrV2" -> drughdrV2,
      "drughdrV3" -> drughdrV3,
      "drughdrV4" -> drughdrV4)

    var outFile = ""

    for ((key,value) <- drugList) {
      for (a <- value) {
        var drugRdd = sc.textFile(a)
        try {
          outFile = a.reverse.split('/')(0).split('.')(1).reverse
        }
        catch {
          case e: Exception => println(a.toString + " - Exception");
        }
        if (outFile != null) {
          try {
            var drugRddMap = drugRdd.map(line => {
              var tokens = line.split('$')
              if (tokens.length >= 5 && !line.contains("ISR$DRUG_SEQ$ROLE_COD$DRUGNAME") && !line.contains("primaryid$caseid$drug_seq$role_cod$drugname")) {
                if (key.toString().contains("drughdrV1") || key.toString().contains("drughdrV2")) {
                  tokens(0) + "\t" + tokens(3)
                }
                else if (key.toString().contains("drughdrV3") || key.toString().contains("drughdrV4")) {
                  tokens(0) + "\t" + tokens(4)
                }
                else {
                  ""
                }
              }
              else {
                ""
              }
            })
            drugRddMap.saveAsTextFile("/data/aers/msfx/drug/" + outFile)
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
