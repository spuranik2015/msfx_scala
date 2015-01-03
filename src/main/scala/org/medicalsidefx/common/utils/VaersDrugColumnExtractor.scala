package org.medicalsidefx.common.utils

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by sanjay on 12/23/14.
 */
object VaersDrugColumnExtractor {
  def main(args: Array[String]) {
    val sconf = new SparkConf().setMaster("local").setAppName("MedicalSideFx-VaersDrugColumnExtractor")
    val sc = new SparkContext(sconf)

    val drughdrV1 = List("/data/vaers/data/1990VAERSData/1990VAERSVAX.csv","/data/vaers/data/1991VAERSData/1991VAERSVAX.csv","/data/vaers/data/1992VAERSData/1992VAERSVAX.csv","/data/vaers/data/1993VAERSData/1993VAERSVAX.csv","/data/vaers/data/1994VAERSData/1994VAERSVAX.csv","/data/vaers/data/1995VAERSData/1995VAERSVAX.csv","/data/vaers/data/1996VAERSData/1996VAERSVAX.csv","/data/vaers/data/1997VAERSData/1997VAERSVAX.csv","/data/vaers/data/1998VAERSData/1998VAERSVAX.csv","/data/vaers/data/1999VAERSData/1999VAERSVAX.csv","/data/vaers/data/2000VAERSData/2000VAERSVAX.csv","/data/vaers/data/2001VAERSData/2001VAERSVAX.csv","/data/vaers/data/2002VAERSData/2002VAERSVAX.csv","/data/vaers/data/2003VAERSData/2003VAERSVAX.csv","/data/vaers/data/2004VAERSData/2004VAERSVAX.csv","/data/vaers/data/2005VAERSData/2005VAERSVAX.csv","/data/vaers/data/2006VAERSData/2006VAERSVAX.csv","/data/vaers/data/2007VAERSData/2007VAERSVAX.csv","/data/vaers/data/2008VAERSData/2008VAERSVAX.csv","/data/vaers/data/2009VAERSData/2009VAERSVAX.csv","/data/vaers/data/2010VAERSData/2010VAERSVAX.csv","/data/vaers/data/2011VAERSData/2011VAERSVAX.csv","/data/vaers/data/2012VAERSData/2012VAERSVAX.csv","/data/vaers/data/2013VAERSData/2013VAERSVAX.csv","/data/vaers/data/2014VAERSData/2014VAERSVAX.csv","/data/vaers/data/NonDomesticVAERSData/NonDomesticVAERSVAX.csv")

    val drugList = Map("drughdrV1" -> drughdrV1)

      for ((key,value) <- drugList) {
          for (a <- value) {
            var outFile = ""
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
                  var tokens = line.split(',')
                  if (tokens.length >= 8 && !line.contains("VAERS_ID,VAX_TYPE")) {
                    (tokens(0) + "\t" + tokens(1) + "\t" + tokens(7))
                  }
                  else {
                    ("")
                  }
                })
                drugRddMap.saveAsTextFile("/data/vaers/msfx/drug/" + outFile)
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
