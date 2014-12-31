package org.medicalsidefx.common.utils

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by sanjay on 12/23/14.
 */
object VaersReacColumnExtractor {
  def main(args: Array[String]) {
    val sconf = new SparkConf().setMaster("local").setAppName("MedicalSideFx-VaersReacColumnExtractor")
    val sc = new SparkContext(sconf)

    val reachdrV1 = List("/data/vaers/data/1990VAERSData/1990VAERSSYMPTOMS.csv","/data/vaers/data/1991VAERSData/1991VAERSSYMPTOMS.csv","/data/vaers/data/1992VAERSData/1992VAERSSYMPTOMS.csv","/data/vaers/data/1993VAERSData/1993VAERSSYMPTOMS.csv","/data/vaers/data/1994VAERSData/1994VAERSSYMPTOMS.csv","/data/vaers/data/1995VAERSData/1995VAERSSYMPTOMS.csv","/data/vaers/data/1996VAERSData/1996VAERSSYMPTOMS.csv","/data/vaers/data/1997VAERSData/1997VAERSSYMPTOMS.csv","/data/vaers/data/1998VAERSData/1998VAERSSYMPTOMS.csv","/data/vaers/data/1999VAERSData/1999VAERSSYMPTOMS.csv","/data/vaers/data/2000VAERSData/2000VAERSSYMPTOMS.csv","/data/vaers/data/2001VAERSData/2001VAERSSYMPTOMS.csv","/data/vaers/data/2002VAERSData/2002VAERSSYMPTOMS.csv","/data/vaers/data/2003VAERSData/2003VAERSSYMPTOMS.csv","/data/vaers/data/2004VAERSData/2004VAERSSYMPTOMS.csv","/data/vaers/data/2005VAERSData/2005VAERSSYMPTOMS.csv","/data/vaers/data/2006VAERSData/2006VAERSSYMPTOMS.csv","/data/vaers/data/2007VAERSData/2007VAERSSYMPTOMS.csv","/data/vaers/data/2008VAERSData/2008VAERSSYMPTOMS.csv","/data/vaers/data/2009VAERSData/2009VAERSSYMPTOMS.csv","/data/vaers/data/2010VAERSData/2010VAERSSYMPTOMS.csv","/data/vaers/data/2011VAERSData/2011VAERSSYMPTOMS.csv","/data/vaers/data/2012VAERSData/2012VAERSSYMPTOMS.csv","/data/vaers/data/2013VAERSData/2013VAERSSYMPTOMS.csv","/data/vaers/data/2014VAERSData/2014VAERSSYMPTOMS.csv","/data/vaers/data/NonDomesticVAERSData/NonDomesticVAERSSYMPTOMS.csv")

    val reacList = Map("reachdrV1" -> reachdrV1)

      for ((key,value) <- reacList) {
          for (a <- value) {
            var outFile = ""
            var reacRdd = sc.textFile(a)
            try {
              outFile = a.reverse.split('/')(0).split('.')(1).reverse
            }
            catch {
              case e: Exception => println(a.toString + " - Exception");
            }
            if (outFile != null) {
              try {
                reacRdd.map(line => line.split(',')).map(fields => {
                  if (fields.length >= 10 && !fields(0).contains("VAERS_ID")) {
                    (fields(0),(fields(1)+"\t"+fields(3)+"\t"+fields(5)+"\t"+fields(7)+"\t"+fields(9)))
                  }
                  else {
                    ("","")
                  }
                  }).flatMap(str => str._2.split('\t')).filter(line => line.toString.length() > 0).saveAsTextFile("/data/vaers/msfx/reac/" + outFile)
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
