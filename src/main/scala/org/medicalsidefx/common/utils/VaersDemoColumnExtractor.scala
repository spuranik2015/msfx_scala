package org.medicalsidefx.common.utils

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by sanjay on 12/23/14.
 */
object VaersDemoColumnExtractor {
  def main(args: Array[String]) {
    val sconf = new SparkConf().setMaster("local").setAppName("MedicalSideFx-VersDemoColumnExtractor")
    val sc = new SparkContext(sconf)

    val demohdrV1 = List("/data/vaers/data/1990VAERSData/1990VAERSDATA.csv","/data/vaers/data/1991VAERSData/1991VAERSDATA.csv","/data/vaers/data/1992VAERSData/1992VAERSDATA.csv","/data/vaers/data/1993VAERSData/1993VAERSDATA.csv","/data/vaers/data/1994VAERSData/1994VAERSDATA.csv","/data/vaers/data/1995VAERSData/1995VAERSDATA.csv","/data/vaers/data/1996VAERSData/1996VAERSDATA.csv","/data/vaers/data/1997VAERSData/1997VAERSDATA.csv","/data/vaers/data/1998VAERSData/1998VAERSDATA.csv","/data/vaers/data/1999VAERSData/1999VAERSDATA.csv","/data/vaers/data/2000VAERSData/2000VAERSDATA.csv","/data/vaers/data/2001VAERSData/2001VAERSDATA.csv","/data/vaers/data/2002VAERSData/2002VAERSDATA.csv","/data/vaers/data/2003VAERSData/2003VAERSDATA.csv","/data/vaers/data/2004VAERSData/2004VAERSDATA.csv","/data/vaers/data/2005VAERSData/2005VAERSDATA.csv","/data/vaers/data/2006VAERSData/2006VAERSDATA.csv","/data/vaers/data/2007VAERSData/2007VAERSDATA.csv","/data/vaers/data/2008VAERSData/2008VAERSDATA.csv","/data/vaers/data/2009VAERSData/2009VAERSDATA.csv","/data/vaers/data/2010VAERSData/2010VAERSDATA.csv","/data/vaers/data/2011VAERSData/2011VAERSDATA.csv","/data/vaers/data/2012VAERSData/2012VAERSDATA.csv","/data/vaers/data/2013VAERSData/2013VAERSDATA.csv","/data/vaers/data/2014VAERSData/2014VAERSDATA.csv","/data/vaers/data/NonDomesticVAERSData/NonDomesticVAERSDATA.csv")

    val demoList = Map("demohdrV1" -> demohdrV1)

      for ((key,value) <- demoList) {
          for (a <- value) {
            var outFile = ""
            var yyyyMmDd = ""
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
                  var tokens = line.split(',')
                  try {
                    var dateStr =  tokens(1).split("/")
                    yyyyMmDd = dateStr(2) + dateStr(0) + dateStr(1)
                  }
                  catch {
                    case e: Exception => println("FAILED to split date" + a.toString)
                  }
                  if (tokens.length >= 4 && !line.contains("VAERS_ID,RECVDATE,STATE,AGE_YRS")) {
                      tokens(0) + "\t" + yyyyMmDd + "\t" + tokens(3) + "\t" + "YR"
                  }
                  else {
                    ""
                  }
                })
                demoRddMap.saveAsTextFile("/data/vaers/msfx/demo/" + outFile)
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
